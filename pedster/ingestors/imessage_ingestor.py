"""iMessage ingestor for Pedster."""

import os
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from dagster import Config, Field, OpExecutionContext, asset, get_dagster_logger

from pedster.ingestors.base_ingestor import BaseIngestor
from pedster.utils.database import Message, MessageThread, get_db_session, init_db
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData


logger = get_dagster_logger()


class IMessageIngestorConfig(Config):
    """Configuration for iMessage ingestor."""
    
    db_path: str = Field(default="/tmp/pedster_imessage.db")
    imessage_db_path: str = Field(default="~/Library/Messages/chat.db")
    lookback_hours: int = 24
    trigger_word: Optional[str] = None
    include_from_me: bool = True
    include_group_chats: bool = True
    include_unknown_senders: bool = True
    contacts_to_include: List[str] = Field(default_factory=list)
    contacts_to_exclude: List[str] = Field(default_factory=list)
    phone_number: Optional[str] = None
    max_messages: int = 100
    

class IMessageIngestor(BaseIngestor):
    """Ingest data from iMessage history."""
    
    def __init__(
        self,
        name: str = "imessage",
        description: str = "iMessage ingestor",
        source_name: str = "imessage",
        content_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the iMessage ingestor.
        
        Args:
            name: Ingestor name
            description: Ingestor description
            source_name: Source name for data identification
            content_type: Type of content this ingestor produces
            config: Configuration dictionary with:
                - db_path: Path to SQLite database for storing processed messages
                - imessage_db_path: Path to iMessage database (chat.db)
                - lookback_hours: Number of hours to look back for messages
                - trigger_word: Optional trigger word to filter messages
                - include_from_me: Whether to include messages sent by you
                - include_group_chats: Whether to include messages from group chats
                - include_unknown_senders: Whether to include messages from unknown senders
                - contacts_to_include: List of contacts to include (phone numbers or handles)
                - contacts_to_exclude: List of contacts to exclude (phone numbers or handles)
                - phone_number: Your phone number for identifying "from_me" messages
                - max_messages: Maximum number of messages to process per run
        """
        super().__init__(name, description, source_name, content_type, config)
        
        # Set defaults if config is None
        if not self.config:
            self.config = {}
            
        # Create config object
        self.config_obj = IMessageIngestorConfig(**self.config)
        
        # Expand path for iMessage DB
        self.imessage_db_path = os.path.expanduser(self.config_obj.imessage_db_path)
        
        # Initialize database if it doesn't exist
        os.makedirs(os.path.dirname(self.config_obj.db_path), exist_ok=True)
        init_db(self.config_obj.db_path)
        
        # Maintain in-memory cache of processed message IDs
        self.processed_message_ids: Set[int] = set()
    
    @track_metrics
    def ingest(self) -> List[PipelineData]:
        """Ingest messages from iMessage database.
        
        Returns:
            List of PipelineData objects
        """
        logger.info("Ingesting data from iMessage database")
        
        # Check if iMessage database exists
        if not os.path.exists(self.imessage_db_path):
            logger.error(f"iMessage database not found at {self.imessage_db_path}")
            return []
        
        try:
            # Connect to iMessage database
            imessage_conn = self._connect_to_db()
            
            # Get database session for our database
            db_session = get_db_session(self.config_obj.db_path)
            
            # Calculate lookback date
            lookback_time = datetime.now() - timedelta(hours=self.config_obj.lookback_hours)
            
            # Extract messages based on configuration
            new_messages = self._fetch_recent_messages(
                imessage_conn, 
                lookback_time, 
                limit=self.config_obj.max_messages
            )
            
            # Close iMessage connection
            imessage_conn.close()
            
            results = []
            for message_data in new_messages:
                # Skip if already processed (either in database or in memory)
                message_id = message_data["message_id"]
                if message_id in self.processed_message_ids:
                    continue
                
                existing = db_session.query(Message).filter_by(message_id=message_id).first()
                if existing:
                    self.processed_message_ids.add(message_id)
                    continue
                
                # Get thread from database or create new one
                thread = db_session.query(MessageThread).filter_by(thread_id=message_data["thread_id"]).first()
                if not thread:
                    thread = MessageThread(
                        thread_id=message_data["thread_id"],
                        name=message_data["chat_name"] or message_data["display_name"] or message_data["handle_id"],
                        is_group=message_data["is_group"],
                    )
                    db_session.add(thread)
                    db_session.commit()
                
                # Create new message
                message = Message(
                    thread_id=thread.id,
                    message_id=message_id,
                    text=message_data["text"],
                    from_me=message_data["from_me"],
                    sender=message_data["from_me"] and "me" or message_data["handle_id"],
                    date=message_data["date"],
                    processed=False,
                )
                
                db_session.add(message)
                self.processed_message_ids.add(message_id)
                
                # Convert to pipeline data
                results.append(message.to_pipeline_data())
            
            db_session.commit()
            logger.info(f"Ingested {len(results)} new messages from iMessage")
            return results
            
        except sqlite3.Error as e:
            logger.error(f"Error connecting to iMessage database: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error ingesting iMessage data: {str(e)}")
            return []
    
    def _connect_to_db(self) -> sqlite3.Connection:
        """Connect to the iMessage SQLite database.
        
        Returns:
            SQLite connection
        
        Raises:
            FileNotFoundError: If the database file doesn't exist
            sqlite3.Error: If there's an error connecting to the database
        """
        if not os.path.exists(self.imessage_db_path):
            raise FileNotFoundError(f"iMessage database not found at {self.imessage_db_path}")
        
        # Connect to a copy of the database to avoid locking issues
        conn = sqlite3.connect(f"file:{self.imessage_db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _fetch_recent_messages(
        self, 
        conn: sqlite3.Connection, 
        since_time: datetime,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get messages from iMessage database.
        
        Args:
            conn: SQLite connection to iMessage database
            since_time: Time to look back from
            limit: Maximum number of messages to fetch
            
        Returns:
            List of message dictionaries
        """
        # Convert date to iMessage timestamp format
        # iMessage uses Mac Absolute Time: seconds since 2001-01-01 00:00:00 UTC
        mac_timestamp = int((since_time - datetime(2001, 1, 1)).total_seconds() * 1_000_000_000)
        
        # Base query for messages
        query = """
            SELECT
                message.ROWID as message_id,
                message.date as message_date,
                message.text,
                message.is_from_me,
                message.handle_id,
                handle.id as handle_id,
                chat.display_name as chat_name,
                chat.chat_identifier,
                chat.ROWID as chat_id,
                chat.style as chat_style
            FROM
                message
                LEFT JOIN handle ON message.handle_id = handle.ROWID
                LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
                LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
            WHERE
                message.date > ?
                AND message.text IS NOT NULL
        """
        
        # Add filters based on configuration
        params = [mac_timestamp]
        where_clauses = []
        
        # Filter by trigger word if provided
        if self.config_obj.trigger_word:
            where_clauses.append("message.text LIKE ?")
            params.append(f"%{self.config_obj.trigger_word}%")
        
        # Filter by from_me if needed
        if not self.config_obj.include_from_me:
            where_clauses.append("message.is_from_me = 0")
        
        # Filter by group chats if needed
        if not self.config_obj.include_group_chats:
            where_clauses.append("chat.style != 43")  # 43 for group chats
        
        # Filter by unknown senders if needed
        if not self.config_obj.include_unknown_senders:
            where_clauses.append("handle.id IS NOT NULL")
        
        # Filter by specific contacts if provided
        if self.config_obj.contacts_to_include:
            placeholders = ", ".join(["?" for _ in self.config_obj.contacts_to_include])
            where_clauses.append(f"handle.id IN ({placeholders})")
            params.extend(self.config_obj.contacts_to_include)
        
        # Exclude specific contacts if provided
        if self.config_obj.contacts_to_exclude:
            placeholders = ", ".join(["?" for _ in self.config_obj.contacts_to_exclude])
            where_clauses.append(f"handle.id NOT IN ({placeholders})")
            params.extend(self.config_obj.contacts_to_exclude)
        
        # Add where clauses to query if any
        if where_clauses:
            query += " AND " + " AND ".join(where_clauses)
        
        # Order by date
        query += " ORDER BY message.date DESC"
        
        # Add limit
        query += f" LIMIT {limit}"
        
        # Execute query
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        messages = []
        for row in rows:
            # Convert Mac timestamp to Python datetime
            mac_date = row["message_date"]
            if mac_date:
                # Convert from Mac Absolute Time to Unix timestamp
                python_date = datetime(2001, 1, 1) + timedelta(seconds=mac_date / 1_000_000_000)
            else:
                python_date = datetime.now()
            
            # Determine display name
            display_name = self._get_contact_name(conn, row["handle_id"])
            
            # Check if it's a group chat
            is_group = row["chat_style"] == 43
            
            # Create message data
            messages.append({
                "message_id": str(row["message_id"]),
                "text": row["text"],
                "from_me": bool(row["is_from_me"]),
                "handle_id": row["handle_id"],
                "display_name": display_name,
                "chat_name": row["chat_name"],
                "thread_id": str(row["chat_id"] or row["handle_id"]),
                "date": python_date,
                "is_group": is_group,
            })
        
        return messages
    
    def _get_contact_name(self, conn: sqlite3.Connection, handle_id: str) -> Optional[str]:
        """Get contact name from handle ID.
        
        Args:
            conn: SQLite connection to iMessage database
            handle_id: Handle ID to look up
            
        Returns:
            Contact name if found, None otherwise
        """
        if not handle_id:
            return None
        
        # Try to find contact name in address book
        try:
            cursor = conn.cursor()
            query = """
                SELECT
                    ABPerson.first as first_name,
                    ABPerson.last as last_name
                FROM
                    ABPersonHandle
                    JOIN ABPerson ON ABPersonHandle.person_id = ABPerson.ROWID
                WHERE
                    ABPersonHandle.handle = ?
                LIMIT 1
            """
            cursor.execute(query, (handle_id,))
            result = cursor.fetchone()
            
            if result:
                first_name = result["first_name"] or ""
                last_name = result["last_name"] or ""
                if first_name or last_name:
                    return f"{first_name} {last_name}".strip()
        except Exception:
            # Table might not exist or other issues
            pass
        
        return None


# Create ingestor instance with default configuration
imessage_ingestor_instance = IMessageIngestor(
    config={
        "db_path": os.path.expanduser("~/pedster_data/imessage.db"),
        "imessage_db_path": os.path.expanduser("~/Library/Messages/chat.db"),
        "lookback_hours": 24,
        "trigger_word": "pedster",
        "include_from_me": True,
        "include_group_chats": True,
        "include_unknown_senders": True,
        "max_messages": 100,
    }
)

# Create asset
@asset(
    name="imessage_data",
    description="iMessage data asset",
    group="ingestors",
    io_manager_key="io_manager",
)
@track_metrics
def imessage_ingestor(context: OpExecutionContext) -> List[PipelineData]:
    """iMessage ingestor asset.
    
    Args:
        context: Dagster execution context
        
    Returns:
        List of PipelineData objects
    """
    # Log configuration
    context.log.info("Running iMessage ingestor")
    
    # Run ingestor
    return imessage_ingestor_instance.ingest()
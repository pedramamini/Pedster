"""iMessage ingestor accessing local SQLite database."""

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from dagster import Config, get_dagster_logger

from pedster.ingestors.base_ingestor import BaseIngestor
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData


logger = get_dagster_logger()


class IMessageIngestorConfig(Config):
    """Configuration for iMessage ingestor."""
    
    db_path: str = os.path.expanduser("~/Library/Messages/chat.db")
    lookback_hours: int = 24
    trigger_word: str = "pedster"
    include_attachments: bool = False
    contacts_only: bool = False
    max_messages: int = 100


class IMessageIngestor(BaseIngestor):
    """Ingestor for iMessages from local SQLite database."""
    
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
            config: Configuration dictionary
        """
        super().__init__(name, description, source_name, content_type, config)
        self.config_obj = IMessageIngestorConfig(**(config or {}))
        self.processed_message_ids: Set[int] = set()
    
    def _connect_to_db(self) -> sqlite3.Connection:
        """Connect to the iMessage SQLite database.
        
        Returns:
            SQLite connection
        
        Raises:
            FileNotFoundError: If the database file doesn't exist
            sqlite3.Error: If there's an error connecting to the database
        """
        if not os.path.exists(self.config_obj.db_path):
            raise FileNotFoundError(f"iMessage database not found at {self.config_obj.db_path}")
        
        # Connect to a copy of the database to avoid locking issues
        conn = sqlite3.connect(f"file:{self.config_obj.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _fetch_recent_messages(self) -> List[Dict[str, Any]]:
        """Fetch recent messages from the iMessage database.
        
        Returns:
            List of message dictionaries
        """
        try:
            conn = self._connect_to_db()
            cursor = conn.cursor()
            
            # Calculate the cutoff time
            cutoff_time = datetime.now() - timedelta(hours=self.config_obj.lookback_hours)
            cutoff_timestamp = int(cutoff_time.timestamp() * 1_000_000_000)  # nanoseconds
            
            # SQL query to fetch messages
            query = """
            SELECT 
                m.ROWID as message_id,
                m.date as timestamp,
                m.text as content,
                h.id as sender_id,
                c.chat_identifier as chat_id,
                h.service as service
            FROM 
                message m
            JOIN 
                handle h ON m.handle_id = h.ROWID
            JOIN 
                chat_message_join cmj ON m.ROWID = cmj.message_id
            JOIN 
                chat c ON cmj.chat_id = c.ROWID
            WHERE 
                m.date > ?
                AND m.text IS NOT NULL
                AND m.text LIKE ?
            ORDER BY 
                m.date DESC
            LIMIT ?
            """
            
            # Execute the query
            trigger_pattern = f"%{self.config_obj.trigger_word}%"
            cursor.execute(query, (cutoff_timestamp, trigger_pattern, self.config_obj.max_messages))
            
            # Process results
            results = []
            for row in cursor.fetchall():
                message_id = row["message_id"]
                
                # Skip already processed messages
                if message_id in self.processed_message_ids:
                    continue
                
                # Convert timestamp to datetime
                timestamp = datetime.fromtimestamp(row["timestamp"] / 1_000_000_000)
                
                # Add to results
                results.append({
                    "message_id": message_id,
                    "timestamp": timestamp,
                    "content": row["content"],
                    "sender_id": row["sender_id"],
                    "chat_id": row["chat_id"],
                    "service": row["service"],
                })
                
                # Mark as processed
                self.processed_message_ids.add(message_id)
            
            conn.close()
            return results
            
        except (sqlite3.Error, FileNotFoundError) as e:
            logger.error(f"Error accessing iMessage database: {str(e)}")
            return []
    
    @track_metrics
    def ingest(self) -> List[PipelineData]:
        """Fetch messages from iMessage database.
        
        Returns:
            List of PipelineData objects
        """
        try:
            messages = self._fetch_recent_messages()
            logger.info(f"Found {len(messages)} new messages with trigger word")
            
            results = []
            for message in messages:
                # Extract the actual request (text after the trigger word)
                content = message["content"]
                trigger_idx = content.lower().find(self.config_obj.trigger_word.lower())
                
                if trigger_idx >= 0:
                    request = content[trigger_idx + len(self.config_obj.trigger_word):].strip()
                    
                    # Only process if there's content after the trigger word
                    if request:
                        metadata = {
                            "message_id": message["message_id"],
                            "timestamp": message["timestamp"].isoformat(),
                            "sender": message["sender_id"],
                            "chat": message["chat_id"],
                        }
                        
                        pipeline_data = self.create_pipeline_data(request, metadata)
                        results.append(pipeline_data)
            
            return results
            
        except Exception as e:
            logger.error(f"Error in iMessage ingestor: {str(e)}")
            return []
"""RSS feed ingestor."""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import feedparser
from dagster import Config, get_dagster_logger

from pedster.ingestors.base_ingestor import BaseIngestor
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData


logger = get_dagster_logger()


class RSSIngestorConfig(Config):
    """Configuration for RSS ingestor."""
    
    feed_urls: List[str]
    cache_entries: bool = True
    max_entries_per_feed: int = 10
    include_content: bool = True
    update_interval: int = 3600  # In seconds


class RSSIngestor(BaseIngestor):
    """Ingestor for RSS/Atom feeds using rssidian."""
    
    def __init__(
        self,
        name: str = "rss",
        description: str = "RSS feed ingestor",
        source_name: str = "rss",
        content_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the RSS ingestor.
        
        Args:
            name: Ingestor name
            description: Ingestor description
            source_name: Source name for data identification
            content_type: Type of content this ingestor produces
            config: Configuration dictionary with feed_urls
        """
        super().__init__(name, description, source_name, content_type, config)
        
        if not config or "feed_urls" not in config:
            raise ValueError("feed_urls must be provided in config")
        
        self.config_obj = RSSIngestorConfig(**(config or {}))
        self.processed_entries: Dict[str, datetime] = {}
    
    @track_metrics
    def ingest(self) -> List[PipelineData]:
        """Fetch and parse RSS feeds.
        
        Returns:
            List of PipelineData objects
        """
        results: List[PipelineData] = []
        
        for feed_url in self.config_obj.feed_urls:
            try:
                logger.info(f"Fetching feed: {feed_url}")
                feed = feedparser.parse(feed_url)
                
                if feed.bozo:
                    logger.warning(f"Feed error: {feed.bozo_exception}")
                
                # Process feed entries
                for entry in feed.entries[:self.config_obj.max_entries_per_feed]:
                    entry_id = entry.get("id", entry.get("link", ""))
                    
                    # Skip if already processed
                    if self.config_obj.cache_entries and entry_id in self.processed_entries:
                        continue
                    
                    # Extract content
                    content = ""
                    if self.config_obj.include_content and "content" in entry:
                        content = entry.content[0].value
                    elif "description" in entry:
                        content = entry.description
                    
                    title = entry.get("title", "No title")
                    link = entry.get("link", "")
                    
                    # Create metadata
                    metadata = {
                        "title": title,
                        "link": link,
                        "feed_title": feed.feed.get("title", "Unknown"),
                        "published": entry.get("published", ""),
                        "author": entry.get("author", "Unknown"),
                        "tags": [tag.term for tag in entry.get("tags", [])],
                    }
                    
                    # Create pipeline data
                    full_content = f"# {title}\n\n{content}\n\nSource: {link}"
                    results.append(self.create_pipeline_data(full_content, metadata))
                    
                    # Mark as processed
                    if self.config_obj.cache_entries:
                        self.processed_entries[entry_id] = datetime.now()
                
                # Sleep briefly to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing feed {feed_url}: {str(e)}")
        
        logger.info(f"Processed {len(results)} new feed entries")
        return results
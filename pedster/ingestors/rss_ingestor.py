"""RSS feed ingestor for Pedster."""

import os
import re
import json
import time
import email.utils
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import feedparser
import requests
from dagster import Config, EnvVar, Field, OpExecutionContext, StringSource, asset, get_dagster_logger
from dateutil import parser as date_parser

from pedster.ingestors.base_ingestor import BaseIngestor
from pedster.utils.database import Article, Feed, get_db_session, init_db
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData


logger = get_dagster_logger()


class RSSIngestorConfig(Config):
    """Configuration for RSS ingestor."""
    
    feed_urls: List[str]
    db_path: str = Field(default="/tmp/pedster_rss.db")
    lookback_days: int = 7
    max_articles_per_feed: int = 25
    jina_enhance_content: bool = True
    

class RSSIngestor(BaseIngestor):
    """Ingestor for RSS/Atom feeds."""
    
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
            config: Configuration dictionary with:
                - db_path: Path to SQLite database
                - feed_urls: List of RSS feed URLs
                - lookback_days: Number of days to look back for articles
                - max_articles_per_feed: Maximum number of articles to process per feed
                - jina_enhance_content: Whether to use Jina.ai to enhance truncated content
        """
        super().__init__(name, description, source_name, content_type, config)
        
        # Set defaults if config is None
        if not self.config:
            self.config = {}
            
        # Ensure feed_urls exists
        if "feed_urls" not in self.config:
            self.config["feed_urls"] = []
            
        # Create config object
        self.config_obj = RSSIngestorConfig(**self.config)
        
        # Initialize database if it doesn't exist
        os.makedirs(os.path.dirname(self.config_obj.db_path), exist_ok=True)
        init_db(self.config_obj.db_path)
    
    @track_metrics
    def ingest(self) -> List[PipelineData]:
        """Ingest data from RSS feeds.
        
        Returns:
            List of PipelineData objects
        """
        feed_urls = self.config_obj.feed_urls
        if not feed_urls:
            logger.warning("No RSS feed URLs provided")
            return []
        
        # Get database session
        db_session = get_db_session(self.config_obj.db_path)
        
        # Process feeds
        results = []
        for feed_url in feed_urls:
            # Get feed from database or create new one
            feed = db_session.query(Feed).filter_by(url=feed_url).first()
            if not feed:
                # Try to get feed metadata
                parsed_feed = feedparser.parse(feed_url)
                feed_title = parsed_feed.feed.get("title", feed_url)
                feed_description = parsed_feed.feed.get("description", "")
                feed_website = parsed_feed.feed.get("link", "")
                
                feed = Feed(
                    title=feed_title,
                    url=feed_url,
                    description=feed_description,
                    website_url=feed_website,
                )
                db_session.add(feed)
                db_session.commit()
            
            # Skip muted feeds
            if feed.muted:
                logger.info(f"Skipping muted feed: {feed.title}")
                continue
            
            # Process feed
            feed_stats = self._process_feed(
                feed,
                db_session,
                lookback_days=self.config_obj.lookback_days,
                max_articles=self.config_obj.max_articles_per_feed,
            )
            
            # Convert new articles to PipelineData
            if feed_stats["new_articles"] > 0:
                # Get unprocessed articles from this feed
                unprocessed = db_session.query(Article).filter_by(
                    feed_id=feed.id, processed=False
                ).all()
                
                for article in unprocessed:
                    results.append(article.to_pipeline_data())
        
        logger.info(f"Ingested {len(results)} articles from RSS feeds")
        return results
    
    def _process_feed(
        self,
        feed: Feed,
        db_session: Any,
        lookback_days: int = 7,
        max_articles: int = 25,
    ) -> Dict[str, int]:
        """Process a feed, fetch new articles and store them in the database.
        
        Args:
            feed: Feed object
            db_session: SQLAlchemy database session
            lookback_days: Number of days to look back for articles
            max_articles: Maximum number of articles to process per feed
            
        Returns:
            Dict with processing statistics
        """
        logger.info(f"Processing feed: {feed.title}")
        
        # Always update the last checked timestamp
        feed.last_checked = datetime.utcnow()
        
        try:
            # Get feed entries
            entries = self._get_feed_entries(feed.url, lookback_days)
            
            # If no entries were found, this might be a temporary issue or a feed that rarely updates
            if not entries:
                feed.no_entries_count = getattr(feed, "no_entries_count", 0) + 1
                
                # If we consistently find no entries for a long time, log a warning
                if feed.no_entries_count > 5:
                    logger.warning(f"Feed {feed.title} has had no entries for {feed.no_entries_count} consecutive checks")
                
                db_session.commit()
                return {"new_articles": 0, "jina_enhanced": 0}
            
            # Reset no entries counter if we found entries
            feed.no_entries_count = 0
            
            # Sort entries by date if possible, newest first
            sorted_entries = []
            for entry in entries:
                published_date = self._parse_published_date(entry)
                sorted_entries.append((entry, published_date or datetime.min))
            
            sorted_entries.sort(key=lambda x: x[1], reverse=True)
            
            # Limit to max_articles
            sorted_entries = sorted_entries[:max_articles]
            
            new_article_count = 0
            jina_enhanced_count = 0
            
            for entry, published_date in sorted_entries:
                guid = entry.get("id", entry.get("link", ""))
                if not guid:
                    continue
                
                # Check if article already exists
                existing_article = db_session.query(Article).filter_by(guid=guid).first()
                if existing_article:
                    continue
                
                # Extract content from feed entry
                content = self._extract_content(entry)
                
                # Get the article URL - prefer the link field over the guid
                article_url = entry.get("link", guid) if entry.get("link") else guid
                
                # Try to fetch content from Jina.ai in these cases:
                # 1. No content was found in the feed
                # 2. Content is too short (likely truncated)
                # 3. Content appears to be just a summary or snippet
                jina_enhanced = False
                should_fetch_jina = (
                    self.config_obj.jina_enhance_content and (
                        not content or 
                        len(content) < 1000 or  # Content is suspiciously short
                        "[...]".lower() in content.lower() or  # Content contains ellipsis indicating truncation
                        "read more".lower() in content.lower() or  # Content has "read more" prompt
                        "continue reading".lower() in content.lower()  # Content has "continue reading" prompt
                    )
                )
                
                # Check if this is an aggregator feed that needs to peer through to the origin article
                if feed.peer_through and entry.get("description"):
                    origin_url = self._extract_origin_url(entry.get("description", ""))
                    if origin_url and origin_url != article_url:
                        logger.info(f"Feed is marked for peer-through. Using origin URL: {origin_url} instead of {article_url}")
                        article_url = origin_url
                
                if should_fetch_jina and article_url.startswith("http"):
                    logger.info(f"Content may be incomplete for {article_url}, attempting to fetch from Jina.ai")
                    jina_content = self._fetch_jina_content(article_url)
                    
                    if jina_content and (not content or len(jina_content) > len(content)):
                        logger.info(f"Using Jina.ai content for {article_url} - {len(jina_content)} chars vs original {len(content)} chars")
                        content = jina_content
                        jina_enhanced = True
                        jina_enhanced_count += 1
                
                clean_content = self._clean_html(content)
                
                # Create new article
                new_article = Article(
                    feed_id=feed.id,
                    title=entry.get("title", "Untitled"),
                    url=article_url,
                    guid=guid,
                    description=entry.get("summary", ""),
                    content=clean_content,
                    author=entry.get("author", ""),
                    published_at=published_date,
                    fetched_at=datetime.utcnow(),
                    processed=False,
                    word_count=len(clean_content.split()) if clean_content else 0,
                    jina_enhanced=jina_enhanced
                )
                
                logger.info(f"New article: '{new_article.title[:50]}' ({new_article.word_count} words)")
                
                db_session.add(new_article)
                new_article_count += 1
            
            # Update feed last updated timestamp if new articles were found
            if new_article_count > 0:
                feed.last_updated = datetime.utcnow()
                feed.article_count += new_article_count
            
            # Reset error count if successful
            feed.error_count = 0
            feed.last_error = None
            
            db_session.commit()
            return {"new_articles": new_article_count, "jina_enhanced": jina_enhanced_count}
        
        except Exception as e:
            logger.error(f"Error processing feed {feed.title}: {str(e)}")
            # Update error information
            feed.error_count = getattr(feed, "error_count", 0) + 1
            feed.last_error = str(e)
            
            # Check if we should mark the feed as inactive due to too many errors
            if feed.error_count >= 5:  # 5 consecutive errors
                logger.warning(f"Feed {feed.title} has had {feed.error_count} consecutive errors. Marking as muted.")
                feed.muted = True
                feed.muted_reason = f"Auto-muted after {feed.error_count} consecutive errors. Last error: {feed.last_error}"
            
            db_session.commit()
            return {"new_articles": 0, "jina_enhanced": 0}
    
    def _get_feed_entries(self, feed_url: str, lookback_days: int = 7) -> List[Dict[str, Any]]:
        """Fetch and parse a feed, returning entries within lookback period.
        
        Args:
            feed_url: URL of the RSS feed
            lookback_days: Number of days to look back for articles
            
        Returns:
            List of feed entries within the lookback period
        """
        for attempt in range(3):  # Max 3 retries
            try:
                # Add a user agent to avoid 403 errors
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
                
                # Use requests to get the content first to handle redirects and HTTP errors better
                response = None
                try:
                    response = requests.get(feed_url, headers=headers, timeout=30)
                    response.raise_for_status()
                    feed_data = feedparser.parse(response.content)
                except requests.exceptions.RequestException as req_err:
                    # If requests fails, try direct parsing as fallback
                    logger.warning(f"Request failed for {feed_url}, trying direct parsing: {str(req_err)}")
                    feed_data = feedparser.parse(feed_url)
                
                # Check if the feed has a bozo exception (parsing error)
                if hasattr(feed_data, "bozo") and feed_data.bozo and hasattr(feed_data, "bozo_exception"):
                    # Some bozo exceptions are acceptable, others indicate real problems
                    if not isinstance(feed_data.bozo_exception, feedparser.CharacterEncodingOverride):
                        logger.warning(f"Feed parsing warning for {feed_url}: {str(feed_data.bozo_exception)}")
                
                # Check HTTP status
                if hasattr(feed_data, "status") and feed_data.status >= 400:
                    error_msg = f"Error fetching feed {feed_url}: HTTP {feed_data.status}"
                    if attempt < 2:  # Not the last attempt
                        logger.warning(f"{error_msg}, retrying in 2 seconds (attempt {attempt+1}/3)")
                        time.sleep(2)
                        continue
                    else:
                        logger.error(error_msg)
                        return []
                
                # Check if feed has entries
                if not feed_data.entries:
                    logger.warning(f"No entries found in feed {feed_url}")
                    return []
                
                # Calculate lookback date
                lookback_date = datetime.utcnow() - timedelta(days=lookback_days)
                
                # Filter entries by date if possible
                recent_entries = []
                for entry in feed_data.entries:
                    published_date = self._parse_published_date(entry)
                    
                    # If no date available, include the entry
                    if not published_date:
                        recent_entries.append(entry)
                        continue
                    
                    # Include entry if it's within lookback period
                    if published_date >= lookback_date:
                        recent_entries.append(entry)
                
                return recent_entries
            
            except Exception as e:
                if attempt < 2:  # Not the last attempt
                    logger.warning(f"Error processing feed {feed_url}: {str(e)}, retrying in 2 seconds (attempt {attempt+1}/3)")
                    time.sleep(2)
                else:
                    logger.error(f"Failed to process feed {feed_url} after 3 attempts: {str(e)}")
                    return []
    
    def _parse_published_date(self, entry: Dict[str, Any]) -> Optional[datetime]:
        """Parse the published date from a feed entry."""
        # First try the pre-parsed date fields from feedparser
        for date_field in ["published_parsed", "updated_parsed", "created_parsed"]:
            if date_field in entry and entry[date_field]:
                try:
                    time_struct = entry[date_field]
                    # Validate the time struct to avoid out of range errors
                    if time_struct and len(time_struct) >= 9:
                        year = time_struct[0]
                        # Basic validation to avoid mktime errors
                        if 1970 <= year <= 2100:  # Reasonable year range
                            try:
                                return datetime.fromtimestamp(time.mktime(time_struct))
                            except (OverflowError, ValueError) as e:
                                logger.debug(f"Time struct conversion error: {str(e)} for {time_struct}")
                except (TypeError, ValueError) as e:
                    logger.debug(f"Error parsing {date_field}: {str(e)}")
                    continue
        
        # Try string date fields if parsed fields not available or failed
        for date_field in ["published", "updated", "created"]:
            if date_field in entry and entry[date_field] and isinstance(entry[date_field], str):
                date_str = entry[date_field].strip()
                
                # Try multiple date parsing approaches
                parsers = [
                    # ISO format with Z timezone
                    lambda d: datetime.fromisoformat(d.replace("Z", "+00:00")),
                    # RFC 2822 format using email.utils
                    lambda d: datetime.fromtimestamp(time.mktime(email.utils.parsedate(d))),
                    # Use dateutil parser as a fallback
                    lambda d: date_parser.parse(d),
                ]
                
                for parser in parsers:
                    try:
                        return parser(date_str)
                    except (ValueError, TypeError, AttributeError, OverflowError):
                        continue
        
        return None
    
    def _extract_content(self, entry: Dict[str, Any]) -> str:
        """Extract the main content from a feed entry."""
        content = ""
        
        # Try different content fields in order of preference
        # Some feeds put the full content in the 'content' field
        if "content" in entry and entry["content"]:
            for content_item in entry["content"]:
                if "value" in content_item:
                    content = content_item["value"]
                    # If content is substantial, return it
                    if len(content) > 500:  # Arbitrary threshold for "substantial" content
                        return content
        
        # Try other common fields
        if not content and "summary_detail" in entry and "value" in entry["summary_detail"]:
            content = entry["summary_detail"]["value"]
        
        if not content and "summary" in entry:
            content = entry["summary"]
        
        if not content and "description" in entry:
            content = entry["description"]
        
        # Some feeds include content in non-standard fields
        if not content and "content_encoded" in entry:
            content = entry["content_encoded"]
        
        # Some feeds include content in the 'value' field directly
        if not content and "value" in entry:
            content = entry["value"]
            
        # Log if we couldn't find any content
        if not content:
            logger.warning(f"No content found in feed entry: {entry.get('title', 'Unknown title')}")
            # Print available fields for debugging
            logger.debug(f"Available fields: {list(entry.keys())}")
        
        return content
    
    def _extract_origin_url(self, description: str) -> Optional[str]:
        """Extract the origin article URL from a feed description.
        
        Args:
            description: The description text from the feed entry
            
        Returns:
            The origin URL if found, None otherwise
        """
        if not description:
            return None
        
        # Common patterns for finding URLs in descriptions
        # Pattern for <a href="URL">...
        href_pattern = re.compile(r'<a\s+(?:[^>]*?\s+)?href=(["\'])(https?://[^"\'>]+)\1', re.IGNORECASE)
        href_matches = href_pattern.findall(description)
        
        # If we found href links, return the first one (usually the main article link)
        if href_matches and len(href_matches) > 0:
            return href_matches[0][1]  # Return the URL part from the first match
        
        # Pattern for bare URLs
        url_pattern = re.compile(r'https?://[^\s<>"\')]+', re.IGNORECASE)
        url_matches = url_pattern.findall(description)
        
        # If we found bare URLs, return the first one
        if url_matches and len(url_matches) > 0:
            return url_matches[0]
        
        return None
    
    def _clean_html(self, content: str) -> str:
        """Clean HTML or markdown content for storage.
        
        Args:
            content: HTML or markdown content to clean
            
        Returns:
            Cleaned text content
        """
        # If content is empty, return empty string
        if not content:
            return ""
        
        # Check if content is likely markdown (from Jina.ai)
        markdown_indicators = [
            content.startswith('#'),  # Headers
            '**' in content,  # Bold
            '*' in content,  # Italic or list
            '```' in content,  # Code blocks
            '>' in content,  # Blockquotes
            '- ' in content,  # Lists
            '[' in content and '](' in content  # Links
        ]
        
        if any(markdown_indicators):
            # For markdown, preserve it but clean up excessive whitespace
            # Remove any HTML tags that might be mixed in
            content = re.sub(r'<[^>]+>', ' ', content)
            # Fix common Jina.ai artifacts
            content = re.sub(r'\[\s*\]\(\s*\)', '', content)  # Empty links
            content = re.sub(r'\[\s*\]\(http[^)]+\)', '', content)  # Links with empty text
            # Remove excessive newlines (more than 2 in a row)
            content = re.sub(r'\n{3,}', '\n\n', content)
            # Remove excessive whitespace
            content = re.sub(r'\s{2,}', ' ', content)
            return content.strip()
        
        # For HTML content
        # More comprehensive HTML cleaning
        # Remove script and style elements completely
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL)
        # Remove HTML comments
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
        # Replace common HTML entities
        content = content.replace('&nbsp;', ' ')
        content = content.replace('&amp;', '&')
        content = content.replace('&lt;', '<')
        content = content.replace('&gt;', '>')
        content = content.replace('&quot;', '"')
        content = content.replace('&apos;', "'")
        # Remove all remaining HTML tags
        clean_text = re.sub(r'<[^>]+>', ' ', content)
        # Normalize whitespace
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        return clean_text

    def _fetch_jina_content(self, url: str) -> Optional[str]:
        """Fetch content from a URL using Jina.ai's content extraction service.
        
        Args:
            url: The URL to fetch content from
            
        Returns:
            Markdown content or None if fetching failed
        """
        start_time = time.time()
        logger.info(f"Fetching content from Jina.ai for URL: {url}")
        
        try:
            # Use direct URL to ensure proper content extraction
            jina_url = f"https://r.jina.ai/{url}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }
            
            jina_start_time = time.time()
            logger.debug(f"Sending request to Jina.ai: {jina_url}")
            response = requests.get(jina_url, headers=headers, timeout=15)
            jina_duration = time.time() - jina_start_time
            
            if response.status_code == 200:
                # Jina.ai returns the content in markdown format
                content = response.text
                
                # Verify we actually got content and not just a tiny response
                if len(content) < 100:
                    logger.warning(f"Jina.ai returned suspiciously short content ({len(content)} chars) for URL: {url} in {jina_duration:.2f}s")
                    # Try direct URL as fallback
                    direct_start_time = time.time()
                    logger.info(f"Trying direct URL as fallback: {url}")
                    direct_response = requests.get(url, headers=headers, timeout=15)
                    direct_duration = time.time() - direct_start_time
                    
                    if direct_response.status_code == 200 and len(direct_response.text) > len(content):
                        logger.info(f"Using direct URL content instead of Jina.ai for URL: {url} ({len(direct_response.text)} chars, fetched in {direct_duration:.2f}s)")
                        content = direct_response.text
                    else:
                        logger.info(f"Direct URL fallback didn't provide better content ({len(direct_response.text)} chars) in {direct_duration:.2f}s")
                
                total_duration = time.time() - start_time
                logger.info(f"Successfully fetched content from Jina.ai for URL: {url} ({len(content)} chars) in {total_duration:.2f}s")
                return content
            else:
                logger.warning(f"Failed to fetch content from Jina.ai for URL: {url}. Status code: {response.status_code} after {jina_duration:.2f}s")
                
                # Try direct URL as fallback
                try:
                    direct_start_time = time.time()
                    logger.info(f"Trying direct URL as fallback: {url}")
                    direct_response = requests.get(url, headers=headers, timeout=15)
                    direct_duration = time.time() - direct_start_time
                    
                    if direct_response.status_code == 200:
                        logger.info(f"Using direct URL content as fallback for URL: {url} ({len(direct_response.text)} chars, fetched in {direct_duration:.2f}s)")
                        return direct_response.text
                    else:
                        logger.warning(f"Direct URL fallback failed with status code: {direct_response.status_code} after {direct_duration:.2f}s")
                except Exception as direct_err:
                    logger.warning(f"Direct URL fallback also failed for URL: {url}. Error: {str(direct_err)}")
                
                total_duration = time.time() - start_time
                logger.warning(f"All content fetching attempts failed for URL: {url} after {total_duration:.2f}s")
                return None
        except Exception as e:
            total_duration = time.time() - start_time
            logger.warning(f"Error fetching content from Jina.ai for URL: {url}. Error: {str(e)} after {total_duration:.2f}s")
            return None


# Create ingestor instance with default configuration
rss_ingestor_instance = RSSIngestor(
    config={
        "db_path": os.path.expanduser("~/pedster_data/rss.db"),
        "feed_urls": [],
        "lookback_days": 7,
        "max_articles_per_feed": 25,
        "jina_enhance_content": True,
    }
)

# Create asset
@asset(
    name="rss_data",
    description="RSS feed data asset",
    group="ingestors",
    io_manager_key="io_manager",
)
@track_metrics
def rss_ingestor(context: OpExecutionContext, feed_urls: Optional[List[str]] = None) -> List[PipelineData]:
    """RSS feed ingestor asset.
    
    Args:
        context: Dagster execution context
        feed_urls: Optional list of feed URLs to fetch. If not provided, the ingestor will use its configured feeds.
        
    Returns:
        List of PipelineData objects
    """
    # Get ingestor instance
    ingestor = rss_ingestor_instance
    
    # Update feed URLs if provided
    if feed_urls:
        ingestor.config["feed_urls"] = feed_urls
    
    # Log configuration
    context.log.info(f"Running RSS ingestor with {len(ingestor.config['feed_urls'])} feeds")
    
    # Run ingestor
    return ingestor.ingest()
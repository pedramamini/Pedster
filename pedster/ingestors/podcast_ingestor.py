"""Podcast ingestor for Pedster."""

import os
import json
import time
import tempfile
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import feedparser
import requests
from dagster import Config, EnvVar, Field, OpExecutionContext, StringSource, asset, get_dagster_logger

from pedster.ingestors.base_ingestor import BaseIngestor
from pedster.utils.database import Episode, Podcast, get_db_session, init_db
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData


logger = get_dagster_logger()


class PodcastIngestorConfig(Config):
    """Configuration for podcast ingestor."""
    
    feed_urls: List[str]
    db_path: str = Field(default="/tmp/pedster_podcasts.db")
    lookback_days: int = 7
    download_audio: bool = True
    transcribe_audio: bool = True
    whisper_model: str = Field(default="base")
    whisper_language: Optional[str] = None
    whisper_cpu_only: bool = Field(default=True)
    whisper_threads: int = Field(default=4)
    

class PodcastIngestor(BaseIngestor):
    """Ingestor for podcast feeds."""
    
    def __init__(
        self,
        name: str = "podcast",
        description: str = "Podcast feed ingestor",
        source_name: str = "podcast",
        content_type: ContentType = ContentType.AUDIO,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the podcast ingestor.
        
        Args:
            name: Ingestor name
            description: Ingestor description
            source_name: Source name for data identification
            content_type: Type of content this ingestor produces
            config: Configuration dictionary with:
                - db_path: Path to SQLite database
                - feed_urls: List of podcast feed URLs
                - lookback_days: Number of days to look back for episodes
                - download_audio: Whether to download audio files
                - transcribe_audio: Whether to transcribe audio files using Whisper
                - whisper_model: Whisper model to use (tiny, base, small, medium, large)
                - whisper_language: Optional language code for transcription
                - whisper_cpu_only: Whether to use CPU only for transcription
                - whisper_threads: Number of CPU threads for transcription
        """
        super().__init__(name, description, source_name, content_type, config)
        
        # Set defaults if config is None
        if not self.config:
            self.config = {}
            
        # Ensure feed_urls exists
        if "feed_urls" not in self.config:
            self.config["feed_urls"] = []
            
        # Create config object
        self.config_obj = PodcastIngestorConfig(**self.config)
        
        # Initialize database if it doesn't exist
        os.makedirs(os.path.dirname(self.config_obj.db_path), exist_ok=True)
        init_db(self.config_obj.db_path)
        
        # Initialize transcription model lazily
        self._whisper_model = None
    
    @track_metrics
    def ingest(self) -> List[PipelineData]:
        """Ingest data from podcast feeds.
        
        Returns:
            List of PipelineData objects
        """
        feed_urls = self.config_obj.feed_urls
        if not feed_urls:
            logger.warning("No podcast feed URLs provided")
            return []
        
        # Get database session
        db_session = get_db_session(self.config_obj.db_path)
        
        # Process feeds
        results = []
        for feed_url in feed_urls:
            # Get podcast from database or create new one
            podcast = db_session.query(Podcast).filter_by(feed_url=feed_url).first()
            if not podcast:
                # Try to get podcast metadata
                parsed_feed = feedparser.parse(feed_url)
                podcast_title = parsed_feed.feed.get("title", feed_url)
                podcast_author = parsed_feed.feed.get("author", "")
                
                podcast = Podcast(
                    title=podcast_title,
                    author=podcast_author,
                    feed_url=feed_url,
                )
                db_session.add(podcast)
                db_session.commit()
            
            # Skip muted podcasts
            if podcast.muted:
                logger.info(f"Skipping muted podcast: {podcast.title}")
                continue
            
            # Process podcast feed
            podcast_stats = self._process_podcast(
                podcast,
                db_session,
                lookback_days=self.config_obj.lookback_days,
            )
            
            # Convert new episodes to PipelineData
            if podcast_stats["new_episodes"] > 0:
                # Get unprocessed episodes from this podcast
                unprocessed = db_session.query(Episode).filter_by(
                    podcast_id=podcast.id, processed=False
                ).all()
                
                for episode in unprocessed:
                    results.append(episode.to_pipeline_data())
        
        logger.info(f"Ingested {len(results)} episodes from podcast feeds")
        return results
    
    def _process_podcast(
        self,
        podcast: Podcast,
        db_session: Any,
        lookback_days: int = 7,
    ) -> Dict[str, int]:
        """Process a podcast feed, fetch new episodes and store them in the database.
        
        Args:
            podcast: Podcast object
            db_session: SQLAlchemy database session
            lookback_days: Number of days to look back for episodes
            
        Returns:
            Dict with processing statistics
        """
        logger.info(f"Processing podcast: {podcast.title}")
        
        try:
            # Parse feed
            feed = feedparser.parse(podcast.feed_url)
            if not feed.entries:
                logger.warning(f"No entries found in podcast feed: {podcast.feed_url}")
                return {"new_episodes": 0, "transcribed": 0}
            
            # Calculate lookback date
            lookback_date = datetime.utcnow() - timedelta(days=lookback_days)
            
            # First pass: collect recent entries
            recent_entries = []
            for entry in feed.entries:
                # Parse published date
                published_at = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    try:
                        published_at = datetime(*entry.published_parsed[:6])
                    except (ValueError, TypeError):
                        pass
                
                # Include if published within lookback period or no date available
                if not published_at or published_at >= lookback_date:
                    recent_entries.append((entry, published_at or datetime.min))
            
            # Sort entries by date, newest first
            recent_entries.sort(key=lambda x: x[1], reverse=True)
            
            new_episode_count = 0
            transcribed_count = 0
            
            # Process recent entries
            for entry, published_at in recent_entries:
                # Get episode unique identifier
                guid = entry.get("id", entry.get("link", ""))
                if not guid:
                    continue
                
                # Skip if episode already exists
                existing = db_session.query(Episode).filter_by(guid=guid).first()
                if existing:
                    continue
                
                # Extract episode title
                title = entry.get("title")
                if not title:
                    logger.warning(f"Episode missing title in podcast {podcast.title}")
                    continue
                
                # Find audio URL and transcript URL
                audio_url = None
                transcript_url = None
                
                # Look for audio and transcript links
                for link in entry.get("links", []):
                    link_type = link.get("type", "").lower()
                    if link_type.startswith("audio/"):
                        audio_url = link["href"]
                    elif link_type in ["application/json", "text/vtt", "text/srt", "text/plain"] or \
                         link.get("rel", "") == "transcript" or \
                         "transcript" in link.get("href", "").lower():
                        transcript_url = link["href"]
                
                # Also check for enclosures (common for podcasts)
                if not audio_url and "enclosures" in entry:
                    for enclosure in entry.get("enclosures", []):
                        enclosure_type = enclosure.get("type", "").lower()
                        if enclosure_type.startswith("audio/"):
                            audio_url = enclosure.get("href")
                            break
                
                # Skip if no audio URL found
                if not audio_url:
                    logger.warning(f"No audio URL found for episode: {title}")
                    continue
                
                # Create new episode
                episode = Episode(
                    podcast_id=podcast.id,
                    guid=guid,
                    title=title,
                    description=entry.get("description", ""),
                    published_at=published_at if published_at != datetime.min else None,
                    audio_url=audio_url,
                    transcript_url=transcript_url,
                    created_at=datetime.utcnow(),
                    processed=False,
                )
                
                # Handle transcription if enabled
                if self.config_obj.download_audio and self.config_obj.transcribe_audio:
                    try:
                        # Download audio
                        logger.info(f"Downloading audio for episode: {episode.title}")
                        temp_path = self._download_audio(audio_url)
                        
                        # Try external transcript first if available
                        if episode.transcript_url:
                            try:
                                logger.info(f"Using external transcript from {episode.transcript_url}")
                                episode.transcript = self._download_transcript(episode.transcript_url)
                                episode.transcript_source = "external"
                            except Exception as e:
                                logger.warning(f"Failed to use external transcript, falling back to Whisper: {str(e)}")
                                episode.transcript_url = None
                        
                        # If no external transcript or it failed, use Whisper
                        if not episode.transcript_url:
                            logger.info(f"Transcribing audio for episode: {episode.title}")
                            episode.transcript = self._transcribe_audio(
                                temp_path, 
                                episode.title,
                                language=self.config_obj.whisper_language,
                            )
                            episode.transcript_source = "whisper"
                            transcribed_count += 1
                        
                        # Cleanup temporary file
                        os.unlink(temp_path)
                    except Exception as e:
                        logger.error(f"Error processing episode {episode.title}: {str(e)}")
                        continue
                
                db_session.add(episode)
                new_episode_count += 1
            
            db_session.commit()
            return {"new_episodes": new_episode_count, "transcribed": transcribed_count}
            
        except Exception as e:
            logger.error(f"Error processing podcast {podcast.title}: {str(e)}")
            return {"new_episodes": 0, "transcribed": 0}
    
    def _download_audio(self, url: str) -> str:
        """Download audio file to temporary location.
        
        Args:
            url: URL of the audio file
            
        Returns:
            Path to downloaded temporary file
        """
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Create temp file with .mp3 extension for whisper
        temp = tempfile.NamedTemporaryFile(suffix=".mp3", delete=False)
        for chunk in response.iter_content(chunk_size=8192):
            temp.write(chunk)
        temp.close()
        return temp.name
    
    def _download_transcript(self, transcript_url: str) -> str:
        """Download and process an external transcript.
        
        Args:
            transcript_url: URL to the transcript file
            
        Returns:
            Processed transcript text
        """
        response = requests.get(transcript_url, timeout=60)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").lower()
        
        # Process different transcript formats
        if "json" in content_type or transcript_url.endswith(".json"):
            # Try to parse JSON transcript (common format)
            try:
                data = response.json()
                # Handle different JSON transcript formats
                if isinstance(data, list) and all("text" in item for item in data):
                    # Format with list of segments with text
                    return " ".join(item["text"] for item in data)
                elif "transcript" in data:
                    # Simple format with transcript field
                    return data["transcript"]
                elif "results" in data and "transcripts" in data["results"]:
                    # AWS Transcribe format
                    return data["results"]["transcripts"][0]["transcript"]
                else:
                    # Unknown format, return the raw text
                    return json.dumps(data)
            except json.JSONDecodeError:
                # If JSON parsing fails, treat as plain text
                return response.text
        elif "text/vtt" in content_type or transcript_url.endswith(".vtt"):
            # WebVTT format
            lines = []
            for line in response.text.splitlines():
                # Skip WebVTT headers, timestamps, and empty lines
                if not line.strip() or line.startswith("WEBVTT") or "-->" in line or line[0].isdigit():
                    continue
                lines.append(line)
            return " ".join(lines)
        elif "text/srt" in content_type or transcript_url.endswith(".srt"):
            # SRT format
            lines = []
            for line in response.text.splitlines():
                # Skip SRT headers, timestamps, and empty lines
                if not line.strip() or "-->" in line or line[0].isdigit():
                    continue
                lines.append(line)
            return " ".join(lines)
        else:
            # Default to plain text
            return response.text
    
    def _load_whisper_model(self):
        """Lazy load the Whisper model with configured options."""
        if self._whisper_model is None:
            try:
                import torch
                import whisper
                
                # Set tokenizers parallelism
                os.environ["TOKENIZERS_PARALLELISM"] = "false"
                
                # Set device based on config and availability
                if self.config_obj.whisper_cpu_only:
                    device = "cpu"
                    if self.config_obj.whisper_threads > 0:
                        torch.set_num_threads(self.config_obj.whisper_threads)
                        logger.info(f"Set torch threads to {self.config_obj.whisper_threads}")
                else:
                    device = "cuda" if torch.cuda.is_available() else "cpu"
                    logger.info(f"Using device: {device}")
                
                self._whisper_model = whisper.load_model(
                    self.config_obj.whisper_model,
                    device=device
                )
                
                logger.info(f"Loaded Whisper model: {self.config_obj.whisper_model}")
            except ImportError:
                raise ImportError("Whisper or torch not installed. Please install them with: pip install whisper torch")
    
    def _transcribe_audio(self, audio_path: str, title: str, language: Optional[str] = None) -> str:
        """Transcribe audio file using Whisper.
        
        Args:
            audio_path: Path to audio file
            title: Episode title (for logging)
            language: Optional language code for transcription
            
        Returns:
            Transcribed text
        """
        # Load model if not already loaded
        self._load_whisper_model()
        
        # Prepare transcription options
        options = {}
        if language:
            options["language"] = language
        
        if self.config_obj.whisper_threads > 0:
            options["num_threads"] = self.config_obj.whisper_threads
        
        logger.info(f"Starting transcription of {title}")
        
        # Perform transcription
        start_time = time.time()
        result = self._whisper_model.transcribe(audio_path, **options)
        duration = time.time() - start_time
        
        logger.info(f"Transcription completed in {duration:.2f}s")
        
        return result["text"]


# Create ingestor instance with default configuration
podcast_ingestor_instance = PodcastIngestor(
    config={
        "db_path": os.path.expanduser("~/pedster_data/podcasts.db"),
        "feed_urls": [],
        "lookback_days": 7,
        "download_audio": True,
        "transcribe_audio": True,
        "whisper_model": "base",
        "whisper_cpu_only": True,
        "whisper_threads": 4,
    }
)

# Create asset
@asset(
    name="podcast_data",
    description="Podcast feed data asset",
    group="ingestors",
    io_manager_key="io_manager",
)
@track_metrics
def podcast_ingestor(context: OpExecutionContext, feed_urls: Optional[List[str]] = None) -> List[PipelineData]:
    """Podcast feed ingestor asset.
    
    Args:
        context: Dagster execution context
        feed_urls: Optional list of feed URLs to fetch. If not provided, the ingestor will use its configured feeds.
        
    Returns:
        List of PipelineData objects
    """
    # Get ingestor instance
    ingestor = podcast_ingestor_instance
    
    # Update feed URLs if provided
    if feed_urls:
        ingestor.config["feed_urls"] = feed_urls
    
    # Log configuration
    context.log.info(f"Running podcast ingestor with {len(ingestor.config['feed_urls'])} feeds")
    
    # Run ingestor
    return ingestor.ingest()
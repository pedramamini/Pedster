"""Ingestors for Pedster."""

from pedster.ingestors.cli_ingestor import CLIIngestor, cli_ingestor
from pedster.ingestors.rss_ingestor import RSSIngestor, rss_ingestor
from pedster.ingestors.podcast_ingestor import PodcastIngestor, podcast_ingestor
from pedster.ingestors.imessage_ingestor import IMessageIngestor, imessage_ingestor
from pedster.ingestors.web_ingestor import WebIngestor, web_ingestor

__all__ = [
    "CLIIngestor", 
    "RSSIngestor", 
    "PodcastIngestor", 
    "IMessageIngestor", 
    "WebIngestor",
    "cli_ingestor",
    "rss_ingestor",
    "podcast_ingestor",
    "imessage_ingestor",
    "web_ingestor",
]
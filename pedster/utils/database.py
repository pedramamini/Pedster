"""Database models and utilities for Pedster."""

import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any, Union

from sqlalchemy import (
    create_engine, Column, Integer, String, 
    Text, DateTime, Boolean, Float, ForeignKey, JSON
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, Session

Base = declarative_base()


class Feed(Base):
    """Model for RSS feed subscriptions."""
    __tablename__ = "feeds"

    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    url = Column(String(512), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    website_url = Column(String(512), nullable=True)
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_checked = Column(DateTime, default=datetime.utcnow)
    muted = Column(Boolean, default=False)
    muted_reason = Column(Text, nullable=True)  # Reason for muting the feed
    peer_through = Column(Boolean, default=False)  # Whether to peer through aggregator to origin article
    error_count = Column(Integer, default=0)  # Count of consecutive errors
    no_entries_count = Column(Integer, default=0)  # Count of consecutive checks with no entries
    last_error = Column(Text, nullable=True)
    
    # Statistics
    article_count = Column(Integer, default=0)  # Total count of ingested articles
    avg_quality_score = Column(Float, nullable=True)  # Average quality score of articles
    quality_tier_counts = Column(Text, nullable=True)  # JSON string of quality tier counts {"S": 5, "A": 10, ...}
    
    # Relationships
    articles = relationship("Article", back_populates="feed", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Feed(id={self.id}, title='{self.title}', muted={self.muted})>"


class Article(Base):
    """Model for articles from RSS feeds."""
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True)
    feed_id = Column(Integer, ForeignKey("feeds.id"), nullable=False)
    title = Column(String(512), nullable=False)
    url = Column(String(512), nullable=False)
    guid = Column(String(512), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    author = Column(String(255), nullable=True)
    published_at = Column(DateTime, nullable=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False)
    processed_at = Column(DateTime, nullable=True)
    summary = Column(Text, nullable=True)
    quality_tier = Column(String(1), nullable=True)  # S, A, B, C, D
    quality_score = Column(Integer, nullable=True)  # 1-100
    labels = Column(String(255), nullable=True)
    embedding_generated = Column(Boolean, default=False)
    embedding = Column(Text, nullable=True)  # JSON string of vector embedding
    word_count = Column(Integer, nullable=True)
    jina_enhanced = Column(Boolean, default=False)  # Flag to indicate if content was fetched from Jina.ai
    
    # Relationships
    feed = relationship("Feed", back_populates="articles")

    def __repr__(self) -> str:
        return f"<Article(id={self.id}, title='{self.title[:30]}...', quality_tier='{self.quality_tier or 'None'}')>"

    def to_pipeline_data(self) -> Dict[str, Any]:
        """Convert to PipelineData format for processing."""
        from pedster.utils.models import ContentType, PipelineData
        
        return PipelineData(
            id=str(self.id),
            content=self.content,
            content_type=ContentType.TEXT,
            source=f"rss:{self.feed.title}",
            timestamp=self.published_at or self.fetched_at,
            metadata={
                "title": self.title,
                "url": self.url,
                "author": self.author,
                "feed_id": self.feed_id,
                "feed_title": self.feed.title,
                "description": self.description,
                "word_count": self.word_count,
                "summary": self.summary,
                "quality_tier": self.quality_tier,
                "quality_score": self.quality_score,
                "labels": self.labels,
                "original_id": self.id,
                "guid": self.guid,
            }
        )


class Podcast(Base):
    """Model for podcast feeds."""
    __tablename__ = "podcasts"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    author = Column(String(255))
    feed_url = Column(String(512), unique=True, nullable=False)
    muted = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    episodes = relationship("Episode", back_populates="podcast")

    def __repr__(self) -> str:
        return f"<Podcast(id={self.id}, title='{self.title}', muted={self.muted})>"


class Episode(Base):
    """Model for podcast episodes."""
    __tablename__ = "episodes"
    
    id = Column(Integer, primary_key=True)
    podcast_id = Column(Integer, ForeignKey("podcasts.id"))
    guid = Column(String(512), unique=True, nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    published_at = Column(DateTime)
    audio_url = Column(String(512))
    transcript = Column(Text)
    transcript_source = Column(String(50))  # Source of transcript: 'whisper', 'external', etc.
    transcript_url = Column(String(512))  # URL to external transcript if available
    embedding = Column(Text)  # JSON string of vector embedding
    summary = Column(Text)  # AI-generated summary
    quality_tier = Column(String(1), nullable=True)  # S, A, B, C, D
    quality_score = Column(Integer, nullable=True)  # 1-100
    processed = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)  # When the episode was processed
    
    # Relationships
    podcast = relationship("Podcast", back_populates="episodes")

    def __repr__(self) -> str:
        return f"<Episode(id={self.id}, title='{self.title[:30]}...', processed={self.processed})>"

    def to_pipeline_data(self) -> Dict[str, Any]:
        """Convert to PipelineData format for processing."""
        from pedster.utils.models import ContentType, PipelineData
        
        return PipelineData(
            id=str(self.id),
            content=self.transcript or self.description,
            content_type=ContentType.TEXT if self.transcript else ContentType.AUDIO,
            source=f"podcast:{self.podcast.title}",
            timestamp=self.published_at or self.created_at,
            metadata={
                "title": self.title,
                "podcast_title": self.podcast.title,
                "podcast_author": self.podcast.author,
                "description": self.description,
                "audio_url": self.audio_url,
                "transcript_source": self.transcript_source,
                "podcast_id": self.podcast_id,
                "original_id": self.id,
                "guid": self.guid,
                "summary": self.summary,
                "quality_tier": self.quality_tier,
                "quality_score": self.quality_score,
            }
        )


class MessageThread(Base):
    """Model for iMessage message threads."""
    __tablename__ = "message_threads"
    
    id = Column(Integer, primary_key=True)
    thread_id = Column(String(255), unique=True, nullable=False)
    name = Column(String(255))
    is_group = Column(Boolean, default=False)
    last_processed = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    messages = relationship("Message", back_populates="thread", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<MessageThread(id={self.id}, name='{self.name}', is_group={self.is_group})>"


class Message(Base):
    """Model for iMessage messages."""
    __tablename__ = "messages"
    
    id = Column(Integer, primary_key=True)
    thread_id = Column(Integer, ForeignKey("message_threads.id"), nullable=False)
    message_id = Column(String(255), unique=True, nullable=False)
    text = Column(Text)
    from_me = Column(Boolean, default=False)
    sender = Column(String(255))
    date = Column(DateTime, nullable=False)
    processed = Column(Boolean, default=False)
    processed_at = Column(DateTime)
    
    # Relationships
    thread = relationship("MessageThread", back_populates="messages")

    def __repr__(self) -> str:
        return f"<Message(id={self.id}, from={'me' if self.from_me else self.sender}, date={self.date})>"

    def to_pipeline_data(self) -> Dict[str, Any]:
        """Convert to PipelineData format for processing."""
        from pedster.utils.models import ContentType, PipelineData
        
        return PipelineData(
            id=str(self.id),
            content=self.text,
            content_type=ContentType.TEXT,
            source=f"imessage:{self.thread.name or self.thread.thread_id}",
            timestamp=self.date,
            metadata={
                "from_me": self.from_me,
                "sender": self.sender,
                "thread_id": self.thread.thread_id,
                "thread_name": self.thread.name,
                "is_group": self.thread.is_group,
                "original_id": self.id,
                "message_id": self.message_id,
            }
        )


def init_db(db_path: str) -> Engine:
    """Initialize database and create all tables."""
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    return engine


def get_db_session(db_path: str) -> Session:
    """Get a database session."""
    engine = create_engine(f"sqlite:///{db_path}")
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()
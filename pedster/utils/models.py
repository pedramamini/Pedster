"""Data models for Pedster pipeline."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class ContentType(str, Enum):
    """Content type enum for data passing through the pipeline."""

    TEXT = "text"
    AUDIO = "audio"
    VIDEO = "video"
    IMAGE = "image"
    MARKDOWN = "markdown"
    URL = "url"
    JSON = "json"


class MetricsData(BaseModel):
    """Metrics data for tracking performance."""

    execution_time_ms: float = 0.0
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    call_count: int = 1
    errors: int = 0


class PipelineData(BaseModel):
    """Base data model for all data passing through the pipeline."""

    id: str = Field(..., description="Unique identifier")
    content: Any = Field(..., description="The actual content")
    content_type: ContentType = Field(..., description="Type of content")
    source: str = Field(..., description="Source of the data")
    timestamp: datetime = Field(default_factory=datetime.now)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    metrics: MetricsData = Field(default_factory=MetricsData)

    def to_json(self) -> Dict[str, Any]:
        """Convert to JSON serializable dictionary."""
        return self.model_dump(mode="json")


class ProcessorResult(BaseModel):
    """Result from a processor operation."""

    data: PipelineData
    success: bool = True
    error_message: Optional[str] = None
    metrics: MetricsData = Field(default_factory=MetricsData)


class MapReduceResult(BaseModel):
    """Result from a map-reduce operation."""

    results: List[ProcessorResult] = Field(default_factory=list)
    combined_content: Optional[Any] = None
    metrics: MetricsData = Field(default_factory=MetricsData)


class ObsidianConfig(BaseModel):
    """Configuration for Obsidian output."""

    vault_path: str
    folder: Optional[str] = None
    file_name: Optional[str] = None
    template: Optional[str] = None
    append: bool = False
    prepend: bool = False
    replace: bool = False
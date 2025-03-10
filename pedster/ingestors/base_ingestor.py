"""Base ingestor class for all ingestors."""

import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from dagster import AssetIn, asset, get_dagster_logger

from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData


logger = get_dagster_logger()


class BaseIngestor(ABC):
    """Base class for all ingestors."""
    
    name: str
    description: str
    source_name: str
    content_type: ContentType
    
    def __init__(
        self,
        name: str,
        description: str,
        source_name: str,
        content_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the ingestor.
        
        Args:
            name: Ingestor name
            description: Ingestor description
            source_name: Source name for data identification
            content_type: Type of content this ingestor produces
            config: Configuration dictionary
        """
        self.name = name
        self.description = description
        self.source_name = source_name
        self.content_type = content_type
        self.config = config or {}
    
    @abstractmethod
    def ingest(self) -> List[PipelineData]:
        """Ingest data from source.
        
        Returns:
            List of PipelineData objects
        """
        pass
    
    @track_metrics
    def create_pipeline_data(
        self, content: Any, metadata: Optional[Dict[str, Any]] = None
    ) -> PipelineData:
        """Create a PipelineData object with unique ID.
        
        Args:
            content: The content to be processed
            metadata: Optional metadata
            
        Returns:
            PipelineData object
        """
        return PipelineData(
            id=str(uuid.uuid4()),
            content=content,
            content_type=self.content_type,
            source=self.source_name,
            metadata=metadata or {},
        )
    
    def get_asset(self, **kwargs: Any) -> Any:
        """Get an asset decorator for this ingestor.
        
        Returns:
            Decorated asset function
        """
        
        @asset(
            name=f"{self.name}_data",
            description=self.description,
            group="ingestors",
            **kwargs,
        )
        @track_metrics
        def _asset() -> List[PipelineData]:
            logger.info(f"Running ingestor: {self.name}")
            return self.ingest()
        
        return _asset
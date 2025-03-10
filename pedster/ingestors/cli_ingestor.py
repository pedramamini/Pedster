"""Command-line ingestor that reads from stdin."""

import sys
from typing import Dict, List, Optional, Any

from dagster import Config, In, OpExecutionContext, get_dagster_logger, op

from pedster.ingestors.base_ingestor import BaseIngestor
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData


logger = get_dagster_logger()


class CLIIngestorConfig(Config):
    """Configuration for CLI ingestor."""
    
    buffer_size: int = 4096
    input_format: str = "text"  # "text", "json", etc.
    max_size: Optional[int] = None
    trim_whitespace: bool = True


class CLIIngestor(BaseIngestor):
    """Ingestor that reads data from command line stdin."""
    
    def __init__(
        self,
        name: str = "cli",
        description: str = "Command-line ingestor that reads from stdin",
        source_name: str = "cli",
        content_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the CLI ingestor.
        
        Args:
            name: Ingestor name
            description: Ingestor description
            source_name: Source name for data identification
            content_type: Type of content this ingestor produces
            config: Configuration dictionary
        """
        super().__init__(name, description, source_name, content_type, config)
        self.config_obj = CLIIngestorConfig(**(config or {}))
    
    @track_metrics
    def ingest(self) -> List[PipelineData]:
        """Read data from stdin.
        
        Returns:
            List containing a PipelineData object with stdin content
        """
        logger.info("Reading from stdin...")
        content = ""
        
        # Read from stdin
        if not sys.stdin.isatty():  # Check if something is being piped in
            content = sys.stdin.read(self.config_obj.max_size)
            if self.config_obj.trim_whitespace:
                content = content.strip()
            
            logger.info(f"Read {len(content)} characters from stdin")
        else:
            logger.warning("No input piped to stdin")
        
        if not content:
            return []
        
        # Create pipeline data
        metadata = {"size": len(content), "source": "stdin"}
        return [self.create_pipeline_data(content, metadata)]


@op(
    description="Ingest data from stdin",
    ins={"text": In(None, description="Trigger input (not used)")},
)
def cli_ingest_op(context: OpExecutionContext) -> List[PipelineData]:
    """Operation to ingest data from stdin.
    
    Args:
        context: Operation execution context
        
    Returns:
        List of PipelineData objects
    """
    ingestor = CLIIngestor()
    result = ingestor.ingest()
    context.log.info(f"Ingested {len(result)} items from CLI")
    return result
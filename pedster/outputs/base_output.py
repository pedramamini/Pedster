"""Base output class for all outputs."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from dagster import AssetIn, OpExecutionContext, asset, get_dagster_logger

from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData, ProcessorResult


logger = get_dagster_logger()


class BaseOutput(ABC):
    """Base class for all outputs."""
    
    name: str
    description: str
    accepted_types: List[ContentType]
    
    def __init__(
        self,
        name: str,
        description: str,
        accepted_types: Union[ContentType, List[ContentType]],
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the output.
        
        Args:
            name: Output name
            description: Output description
            accepted_types: Type(s) of content this output accepts
            config: Configuration dictionary
        """
        self.name = name
        self.description = description
        
        if isinstance(accepted_types, list):
            self.accepted_types = accepted_types
        else:
            self.accepted_types = [accepted_types]
            
        self.config = config or {}
    
    def can_output(self, data: Union[PipelineData, ProcessorResult]) -> bool:
        """Check if this output can handle the given data.
        
        Args:
            data: The data to check
            
        Returns:
            True if this output can handle the data, False otherwise
        """
        if isinstance(data, ProcessorResult):
            data = data.data
            
        return data.content_type in self.accepted_types
    
    @abstractmethod
    def output(self, data: Union[PipelineData, ProcessorResult]) -> bool:
        """Output the given data.
        
        Args:
            data: The data to output
            
        Returns:
            True if output succeeded, False otherwise
        """
        pass
    
    def get_asset(self, input_assets: List[str], **kwargs: Any) -> Any:
        """Get an asset decorator for this output.
        
        Args:
            input_assets: List of input asset names
            **kwargs: Additional keyword arguments for the asset decorator
            
        Returns:
            Decorated asset function
        """
        ins = {asset_name: AssetIn(asset_name) for asset_name in input_assets}
        
        @asset(
            name=f"{self.name}_output",
            description=self.description,
            group="outputs",
            ins=ins,
            **kwargs,
        )
        @track_metrics
        def _asset(context: OpExecutionContext, **inputs: List[Union[PipelineData, ProcessorResult]]) -> List[bool]:
            results = []
            
            # Process all inputs
            for asset_name, data_list in inputs.items():
                for data in data_list:
                    if self.can_output(data):
                        try:
                            context.log.info(f"Outputting data from {asset_name} with {self.name}")
                            success = self.output(data)
                            results.append(success)
                            
                            if success:
                                context.log.info(f"Successfully output data")
                            else:
                                context.log.error(f"Failed to output data")
                                
                        except Exception as e:
                            context.log.error(f"Error outputting data: {str(e)}")
                            results.append(False)
                    else:
                        context.log.warning(
                            f"Output {self.name} cannot handle data of type "
                            f"{data.data.content_type if isinstance(data, ProcessorResult) else data.content_type}"
                        )
            
            return results
        
        return _asset
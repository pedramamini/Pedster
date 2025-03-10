"""Base processor class for all processors."""

import copy
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from dagster import AssetIn, In, OpExecutionContext, asset, get_dagster_logger, op

from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData, ProcessorResult


logger = get_dagster_logger()


class BaseProcessor(ABC):
    """Base class for all processors."""
    
    name: str
    description: str
    input_type: Union[ContentType, List[ContentType]]
    output_type: ContentType
    
    def __init__(
        self,
        name: str,
        description: str,
        input_type: Union[ContentType, List[ContentType]],
        output_type: ContentType,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the processor.
        
        Args:
            name: Processor name
            description: Processor description
            input_type: Type(s) of content this processor accepts
            output_type: Type of content this processor produces
            config: Configuration dictionary
        """
        self.name = name
        self.description = description
        self.input_type = input_type
        self.output_type = output_type
        self.config = config or {}
    
    def can_process(self, data: PipelineData) -> bool:
        """Check if this processor can handle the given data.
        
        Args:
            data: The data to check
            
        Returns:
            True if this processor can handle the data, False otherwise
        """
        if isinstance(self.input_type, list):
            return data.content_type in self.input_type
        return data.content_type == self.input_type
    
    @abstractmethod
    def process(self, data: PipelineData) -> ProcessorResult:
        """Process the given data.
        
        Args:
            data: The data to process
            
        Returns:
            ProcessorResult containing the processed data
        """
        pass
    
    @track_metrics
    def create_result(
        self, 
        data: PipelineData, 
        content: Any = None, 
        content_type: Optional[ContentType] = None,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> ProcessorResult:
        """Create a ProcessorResult with the processed data.
        
        Args:
            data: Original data
            content: New content (if None, original content is used)
            content_type: New content type (if None, output_type is used)
            success: Whether processing succeeded
            error_message: Error message if processing failed
            
        Returns:
            ProcessorResult object
        """
        # Create a copy of the original data
        new_data = copy.deepcopy(data)
        
        # Update content and content_type if provided
        if content is not None:
            new_data.content = content
        
        new_data.content_type = content_type or self.output_type
        
        # Create and return result
        return ProcessorResult(
            data=new_data,
            success=success,
            error_message=error_message,
        )
    
    def get_asset(self, input_assets: List[str], **kwargs: Any) -> Any:
        """Get an asset decorator for this processor.
        
        Args:
            input_assets: List of input asset names
            **kwargs: Additional keyword arguments for the asset decorator
            
        Returns:
            Decorated asset function
        """
        ins = {asset_name: AssetIn(asset_name) for asset_name in input_assets}
        
        @asset(
            name=f"{self.name}_processed",
            description=self.description,
            group="processors",
            ins=ins,
            **kwargs,
        )
        @track_metrics
        def _asset(context: OpExecutionContext, **inputs: List[PipelineData]) -> List[ProcessorResult]:
            results = []
            
            # Process all inputs
            for asset_name, data_list in inputs.items():
                for data in data_list:
                    if self.can_process(data):
                        try:
                            context.log.info(f"Processing data from {asset_name} with {self.name}")
                            result = self.process(data)
                            results.append(result)
                        except Exception as e:
                            context.log.error(f"Error processing data: {str(e)}")
                            results.append(self.create_result(
                                data, 
                                success=False, 
                                error_message=str(e)
                            ))
                    else:
                        context.log.warning(
                            f"Processor {self.name} cannot handle data of type {data.content_type}"
                        )
            
            return results
        
        return _asset
    
    def get_op(self, **kwargs: Any) -> Any:
        """Get an op decorator for this processor.
        
        Args:
            **kwargs: Additional keyword arguments for the op decorator
            
        Returns:
            Decorated op function
        """
        @op(
            name=f"{self.name}_op",
            description=self.description,
            ins={"data": In(PipelineData)},
            **kwargs,
        )
        @track_metrics
        def _op(context: OpExecutionContext, data: PipelineData) -> ProcessorResult:
            if self.can_process(data):
                try:
                    context.log.info(f"Processing data with {self.name}")
                    return self.process(data)
                except Exception as e:
                    context.log.error(f"Error processing data: {str(e)}")
                    return self.create_result(
                        data, 
                        success=False, 
                        error_message=str(e)
                    )
            else:
                context.log.warning(
                    f"Processor {self.name} cannot handle data of type {data.content_type}"
                )
                return self.create_result(
                    data, 
                    success=False, 
                    error_message=f"Cannot handle data of type {data.content_type}"
                )
        
        return _op
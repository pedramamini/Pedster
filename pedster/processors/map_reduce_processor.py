"""Map-Reduce processor for parallel processing with multiple models."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from dagster import Config, get_dagster_logger

from pedster.processors.base_processor import BaseProcessor
from pedster.utils.metrics import track_metrics
from pedster.utils.models import (ContentType, MapReduceResult, PipelineData,
                                  ProcessorResult)


logger = get_dagster_logger()


class MapReduceConfig(Config):
    """Configuration for map-reduce processor."""
    
    max_workers: int = 3
    timeout: int = 120
    combine_results: bool = True
    output_format: str = "markdown"
    parallel: bool = True


class MapReduceProcessor(BaseProcessor):
    """Processor for parallel processing with multiple models."""
    
    def __init__(
        self,
        name: str,
        description: str,
        processors: List[BaseProcessor],
        input_type: ContentType = ContentType.TEXT,
        output_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the map-reduce processor.
        
        Args:
            name: Processor name
            description: Processor description
            processors: List of processors to run in parallel
            input_type: Type of content this processor accepts
            output_type: Type of content this processor produces
            config: Configuration dictionary
        """
        super().__init__(name, description, input_type, output_type, config)
        self.processors = processors
        self.config_obj = MapReduceConfig(**(config or {}))
    
    @track_metrics
    def _process_with_processor(
        self, processor: BaseProcessor, data: PipelineData
    ) -> Tuple[str, ProcessorResult]:
        """Process data with a single processor.
        
        Args:
            processor: The processor to use
            data: The data to process
            
        Returns:
            Tuple of processor name and result
        """
        try:
            logger.info(f"Processing with {processor.name}")
            start_time = time.time()
            
            result = processor.process(data)
            
            execution_time = (time.time() - start_time) * 1000
            logger.info(f"{processor.name} completed in {execution_time:.2f}ms")
            
            return processor.name, result
            
        except Exception as e:
            logger.error(f"Error in {processor.name}: {str(e)}")
            # Return error result
            error_result = processor.create_result(
                data,
                success=False,
                error_message=f"Error in {processor.name}: {str(e)}",
            )
            return processor.name, error_result
    
    def _combine_results(self, results: List[ProcessorResult]) -> str:
        """Combine results from multiple processors.
        
        Args:
            results: List of processor results
            
        Returns:
            Combined content
        """
        if self.config_obj.output_format == "markdown":
            combined = "# Combined Results\n\n"
            
            for result in results:
                processor_name = result.data.metadata.get("processor", "Unknown")
                model_name = result.data.metadata.get("model", "")
                
                # Add section header
                if model_name:
                    combined += f"## {processor_name} ({model_name})\n\n"
                else:
                    combined += f"## {processor_name}\n\n"
                
                # Add content
                combined += f"{result.data.content}\n\n"
                
                # Add metadata
                combined += f"*Processed in {result.metrics.execution_time_ms:.2f}ms*\n\n"
                
                # Add separator
                combined += "---\n\n"
            
            return combined
        else:
            # Simple text concatenation
            return "\n\n".join([r.data.content for r in results])
    
    @track_metrics
    def process(self, data: PipelineData) -> ProcessorResult:
        """Process data with multiple processors in parallel.
        
        Args:
            data: The data to process
            
        Returns:
            ProcessorResult with combined results
        """
        map_reduce_result = MapReduceResult()
        start_time = time.time()
        
        # Add input data information to metadata
        data_copy = data.model_copy(deep=True)
        
        if self.config_obj.parallel:
            # Process in parallel using threads
            results_dict = {}
            
            with ThreadPoolExecutor(max_workers=self.config_obj.max_workers) as executor:
                futures = {
                    executor.submit(
                        self._process_with_processor, processor, data_copy
                    ): processor.name
                    for processor in self.processors
                }
                
                for future in as_completed(futures, timeout=self.config_obj.timeout):
                    processor_name, result = future.result()
                    results_dict[processor_name] = result
                    
                    # Add processor name to metadata
                    result.data.metadata["processor"] = processor_name
                    
                    # Add to results list
                    map_reduce_result.results.append(result)
        else:
            # Process sequentially
            for processor in self.processors:
                processor_name, result = self._process_with_processor(processor, data_copy)
                
                # Add processor name to metadata
                result.data.metadata["processor"] = processor_name
                
                # Add to results list
                map_reduce_result.results.append(result)
        
        # Combine results if requested
        if self.config_obj.combine_results and map_reduce_result.results:
            combined_content = self._combine_results(map_reduce_result.results)
            map_reduce_result.combined_content = combined_content
            
            # Update execution time
            map_reduce_result.metrics.execution_time_ms = (time.time() - start_time) * 1000
            
            # Return combined result
            return self.create_result(
                data_copy,
                content=combined_content,
            )
        elif map_reduce_result.results:
            # Return first successful result
            for result in map_reduce_result.results:
                if result.success:
                    return result
            
            # If no successful results, return first result
            return map_reduce_result.results[0]
        else:
            # No results
            return self.create_result(
                data_copy,
                success=False,
                error_message="No results from any processor",
            )
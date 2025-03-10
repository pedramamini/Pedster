"""Metrics utilities for tracking performance."""

import functools
import time
from typing import Any, Callable, Dict, TypeVar, cast

from dagster import get_dagster_logger
from dagster.core.execution.stats import RunStepKeyStatsSnapshot

from pedster.utils.models import MetricsData, PipelineData, ProcessorResult


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

logger = get_dagster_logger()


def track_metrics(func: F) -> F:
    """Decorator to track execution metrics of a function."""
    
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_time = (time.time() - start_time) * 1000  # Convert to ms
            
            # Update metrics based on return type
            if isinstance(result, PipelineData):
                result.metrics.execution_time_ms = execution_time
                result.metrics.call_count += 1
            elif isinstance(result, ProcessorResult):
                result.metrics.execution_time_ms = execution_time
                result.metrics.call_count += 1
                if hasattr(result, "data") and hasattr(result.data, "metrics"):
                    result.data.metrics.execution_time_ms = execution_time
                    result.data.metrics.call_count += 1
            
            logger.info(
                f"Function {func.__name__} executed in {execution_time:.2f}ms"
            )
            return result
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(
                f"Error in {func.__name__}: {str(e)} after {execution_time:.2f}ms"
            )
            raise
    
    return cast(F, wrapper)


def get_step_metrics(stats: RunStepKeyStatsSnapshot) -> Dict[str, Any]:
    """Extract useful metrics from Dagster step stats."""
    return {
        "step_key": stats.step_key,
        "status": stats.status.value,
        "start_time": stats.start_time,
        "end_time": stats.end_time,
        "duration_ms": (stats.end_time - stats.start_time) * 1000 if stats.end_time else None,
        "attempts": len(stats.attempts),
    }
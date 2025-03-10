"""Tests for data models."""

import json
import unittest
from datetime import datetime

from pedster.utils.models import (ContentType, MapReduceResult, MetricsData,
                                 ObsidianConfig, PipelineData, ProcessorResult)


class TestModels(unittest.TestCase):
    """Test cases for data models."""
    
    def test_pipeline_data(self) -> None:
        """Test PipelineData model."""
        # Create a PipelineData instance
        data = PipelineData(
            id="test-id",
            content="Test content",
            content_type=ContentType.TEXT,
            source="test-source",
            metadata={"key": "value"},
        )
        
        # Check attributes
        self.assertEqual(data.id, "test-id")
        self.assertEqual(data.content, "Test content")
        self.assertEqual(data.content_type, ContentType.TEXT)
        self.assertEqual(data.source, "test-source")
        self.assertEqual(data.metadata, {"key": "value"})
        
        # Check metrics initialization
        self.assertIsInstance(data.metrics, MetricsData)
        self.assertEqual(data.metrics.execution_time_ms, 0.0)
        self.assertEqual(data.metrics.call_count, 1)
        
        # Test to_json method
        json_data = data.to_json()
        self.assertIsInstance(json_data, dict)
        self.assertEqual(json_data["id"], "test-id")
        self.assertEqual(json_data["content"], "Test content")
        self.assertEqual(json_data["content_type"], "text")
        
        # Ensure it's JSON serializable
        json_str = json.dumps(json_data)
        self.assertIsInstance(json_str, str)
    
    def test_processor_result(self) -> None:
        """Test ProcessorResult model."""
        # Create a PipelineData instance
        pipeline_data = PipelineData(
            id="test-id",
            content="Test content",
            content_type=ContentType.TEXT,
            source="test-source",
        )
        
        # Create a ProcessorResult instance
        result = ProcessorResult(
            data=pipeline_data,
            success=True,
            error_message=None,
        )
        
        # Check attributes
        self.assertEqual(result.data, pipeline_data)
        self.assertTrue(result.success)
        self.assertIsNone(result.error_message)
        
        # Check metrics initialization
        self.assertIsInstance(result.metrics, MetricsData)
        self.assertEqual(result.metrics.execution_time_ms, 0.0)
        self.assertEqual(result.metrics.call_count, 1)
    
    def test_map_reduce_result(self) -> None:
        """Test MapReduceResult model."""
        # Create a PipelineData instance
        pipeline_data = PipelineData(
            id="test-id",
            content="Test content",
            content_type=ContentType.TEXT,
            source="test-source",
        )
        
        # Create a ProcessorResult instance
        processor_result = ProcessorResult(
            data=pipeline_data,
            success=True,
        )
        
        # Create a MapReduceResult instance
        map_reduce_result = MapReduceResult(
            results=[processor_result],
            combined_content="Combined content",
        )
        
        # Check attributes
        self.assertEqual(len(map_reduce_result.results), 1)
        self.assertEqual(map_reduce_result.results[0], processor_result)
        self.assertEqual(map_reduce_result.combined_content, "Combined content")
        
        # Check metrics initialization
        self.assertIsInstance(map_reduce_result.metrics, MetricsData)
        self.assertEqual(map_reduce_result.metrics.execution_time_ms, 0.0)
        self.assertEqual(map_reduce_result.metrics.call_count, 1)
    
    def test_obsidian_config(self) -> None:
        """Test ObsidianConfig model."""
        # Create an ObsidianConfig instance
        config = ObsidianConfig(
            vault_path="/path/to/vault",
            folder="notes",
            file_name="test.md",
            template="# {title}\n\n{content}",
            append=True,
        )
        
        # Check attributes
        self.assertEqual(config.vault_path, "/path/to/vault")
        self.assertEqual(config.folder, "notes")
        self.assertEqual(config.file_name, "test.md")
        self.assertEqual(config.template, "# {title}\n\n{content}")
        self.assertTrue(config.append)
        self.assertFalse(config.prepend)
        self.assertFalse(config.replace)


if __name__ == '__main__':
    unittest.main()
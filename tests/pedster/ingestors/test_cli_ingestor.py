"""Tests for CLI ingestor."""

import io
import sys
import unittest
from unittest.mock import patch

from pedster.ingestors.cli_ingestor import CLIIngestor
from pedster.utils.models import ContentType


class TestCLIIngestor(unittest.TestCase):
    """Test cases for CLI ingestor."""
    
    def test_init_default_values(self) -> None:
        """Test initialization with default values."""
        ingestor = CLIIngestor()
        self.assertEqual(ingestor.name, "cli")
        self.assertEqual(ingestor.source_name, "cli")
        self.assertEqual(ingestor.content_type, ContentType.TEXT)
    
    def test_init_custom_values(self) -> None:
        """Test initialization with custom values."""
        ingestor = CLIIngestor(
            name="custom_cli",
            description="Custom CLI",
            source_name="custom_source",
            content_type=ContentType.MARKDOWN,
        )
        self.assertEqual(ingestor.name, "custom_cli")
        self.assertEqual(ingestor.description, "Custom CLI")
        self.assertEqual(ingestor.source_name, "custom_source")
        self.assertEqual(ingestor.content_type, ContentType.MARKDOWN)
    
    @patch('sys.stdin', io.StringIO("Test input"))
    def test_ingest_with_input(self) -> None:
        """Test ingesting with input from stdin."""
        # Need to patch isatty to return False to simulate piped input
        with patch('sys.stdin.isatty', return_value=False):
            ingestor = CLIIngestor()
            result = ingestor.ingest()
            
            # Check results
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].content, "Test input")
            self.assertEqual(result[0].content_type, ContentType.TEXT)
            self.assertEqual(result[0].source, "cli")
            self.assertEqual(result[0].metadata.get("size"), 10)
    
    @patch('sys.stdin.isatty', return_value=True)
    def test_ingest_without_input(self, mock_isatty) -> None:
        """Test ingesting without input from stdin."""
        ingestor = CLIIngestor()
        result = ingestor.ingest()
        
        # No data should be returned
        self.assertEqual(len(result), 0)
    
    def test_config_parsing(self) -> None:
        """Test config parsing."""
        config = {
            "buffer_size": 8192,
            "input_format": "json",
            "max_size": 1000,
            "trim_whitespace": False,
        }
        
        ingestor = CLIIngestor(config=config)
        
        self.assertEqual(ingestor.config_obj.buffer_size, 8192)
        self.assertEqual(ingestor.config_obj.input_format, "json")
        self.assertEqual(ingestor.config_obj.max_size, 1000)
        self.assertEqual(ingestor.config_obj.trim_whitespace, False)


if __name__ == '__main__':
    unittest.main()
"""Web content ingestor using Jina."""

import json
from typing import Any, Dict, List, Optional, Union

import requests
from dagster import Config, get_dagster_logger

from pedster.ingestors.base_ingestor import BaseIngestor
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData


logger = get_dagster_logger()


class WebIngestorConfig(Config):
    """Configuration for web ingestor."""
    
    urls: List[str]
    jina_api_key: Optional[str] = None
    include_images: bool = False
    extract_text: bool = True
    timeout: int = 30
    user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class WebIngestor(BaseIngestor):
    """Ingestor for web content using Jina."""
    
    def __init__(
        self,
        name: str = "web",
        description: str = "Web content ingestor",
        source_name: str = "web",
        content_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the web ingestor.
        
        Args:
            name: Ingestor name
            description: Ingestor description
            source_name: Source name for data identification
            content_type: Type of content this ingestor produces
            config: Configuration dictionary with urls
        """
        super().__init__(name, description, source_name, content_type, config)
        
        if not config or "urls" not in config:
            raise ValueError("urls must be provided in config")
        
        self.config_obj = WebIngestorConfig(**(config or {}))
    
    @track_metrics
    def _extract_content_with_jina(self, url: str) -> Dict[str, Any]:
        """Extract web content using Jina API.
        
        Args:
            url: URL to extract content from
            
        Returns:
            Dictionary with extracted content
            
        Raises:
            requests.RequestException: If there's an error accessing the API
        """
        # This is a placeholder - actual implementation will use Jina
        # You would add the actual Jina SDK implementation here
        
        headers = {
            "User-Agent": self.config_obj.user_agent,
            "Content-Type": "application/json",
        }
        
        if self.config_obj.jina_api_key:
            headers["Authorization"] = f"Bearer {self.config_obj.jina_api_key}"
        
        # For now, just fetch the raw HTML
        try:
            response = requests.get(
                url, 
                headers=headers, 
                timeout=self.config_obj.timeout
            )
            response.raise_for_status()
            
            # Extract title from HTML
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")
            title = soup.title.string if soup.title else "No title"
            
            # Simple text extraction
            text = soup.get_text(separator="\n", strip=True)
            
            return {
                "url": url,
                "title": title,
                "text": text,
                "html": response.text,
                "status_code": response.status_code,
            }
            
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return {
                "url": url,
                "error": str(e),
                "text": "",
                "html": "",
                "status_code": getattr(e.response, "status_code", 0) if hasattr(e, "response") else 0,
            }
    
    @track_metrics
    def ingest(self) -> List[PipelineData]:
        """Fetch and process web content.
        
        Returns:
            List of PipelineData objects
        """
        results = []
        
        for url in self.config_obj.urls:
            try:
                logger.info(f"Fetching content from: {url}")
                content = self._extract_content_with_jina(url)
                
                # Use extracted text or HTML
                text_content = content.get("text", "") if self.config_obj.extract_text else content.get("html", "")
                title = content.get("title", "No title")
                
                # Create formatted content with title
                formatted_content = f"# {title}\n\n{text_content}\n\nSource: {url}"
                
                # Create metadata
                metadata = {
                    "url": url,
                    "title": title,
                    "status_code": content.get("status_code"),
                }
                
                # Add to results
                results.append(self.create_pipeline_data(formatted_content, metadata))
                
            except Exception as e:
                logger.error(f"Error processing {url}: {str(e)}")
        
        logger.info(f"Processed {len(results)} web pages")
        return results
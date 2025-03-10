"""Obsidian markdown output."""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dagster import Config, get_dagster_logger

from pedster.outputs.base_output import BaseOutput
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData, ProcessorResult


logger = get_dagster_logger()


class ObsidianOutputConfig(Config):
    """Configuration for Obsidian output."""
    
    vault_path: str
    folder: str = ""
    file_template: str = "{title}"
    content_template: Optional[str] = None
    create_folders: bool = True
    create_daily_notes: bool = False
    append: bool = False
    prepend: bool = False
    overwrite: bool = False
    add_timestamp: bool = True
    add_frontmatter: bool = True
    tags: List[str] = []


class ObsidianOutput(BaseOutput):
    """Output for Obsidian markdown files."""
    
    def __init__(
        self,
        name: str = "obsidian",
        description: str = "Output to Obsidian markdown files",
        accepted_types: Union[ContentType, List[ContentType]] = [ContentType.TEXT, ContentType.MARKDOWN],
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the Obsidian output.
        
        Args:
            name: Output name
            description: Output description
            accepted_types: Type(s) of content this output accepts
            config: Configuration dictionary with vault_path
        """
        super().__init__(name, description, accepted_types, config)
        
        if not config or "vault_path" not in config:
            raise ValueError("vault_path must be provided in config")
        
        self.config_obj = ObsidianOutputConfig(**(config or {}))
    
    def _ensure_directory_exists(self, directory: str) -> bool:
        """Ensure the directory exists, creating it if necessary.
        
        Args:
            directory: Directory path
            
        Returns:
            True if the directory exists or was created, False otherwise
        """
        if not os.path.exists(directory):
            if self.config_obj.create_folders:
                try:
                    os.makedirs(directory, exist_ok=True)
                    logger.info(f"Created directory: {directory}")
                    return True
                except Exception as e:
                    logger.error(f"Error creating directory {directory}: {str(e)}")
                    return False
            else:
                logger.warning(f"Directory does not exist: {directory}")
                return False
        return True
    
    def _get_file_path(self, data: Union[PipelineData, ProcessorResult]) -> str:
        """Get the file path for the given data.
        
        Args:
            data: The data to output
            
        Returns:
            File path
        """
        # Extract content data
        if isinstance(data, ProcessorResult):
            data = data.data
        
        # Get base path
        base_path = Path(self.config_obj.vault_path)
        
        # Add folder if specified
        if self.config_obj.folder:
            base_path = base_path / self.config_obj.folder
        
        # Create folder if needed
        if not self._ensure_directory_exists(str(base_path)):
            raise OSError(f"Cannot create or access directory: {base_path}")
        
        # Get title for file name
        title = data.metadata.get("title", "Untitled")
        
        # Clean title for filename
        safe_title = "".join(c if c.isalnum() or c in " -_" else "_" for c in title)
        safe_title = safe_title.strip().replace(" ", "_")
        
        # Format file name using template
        file_name = self.config_obj.file_template.format(
            title=safe_title,
            date=datetime.now().strftime("%Y-%m-%d"),
            time=datetime.now().strftime("%H-%M-%S"),
            id=data.id,
            source=data.source,
        )
        
        # Ensure file has .md extension
        if not file_name.endswith(".md"):
            file_name += ".md"
        
        return str(base_path / file_name)
    
    def _format_content(self, data: Union[PipelineData, ProcessorResult]) -> str:
        """Format content for output.
        
        Args:
            data: The data to output
            
        Returns:
            Formatted content
        """
        # Extract content data
        if isinstance(data, ProcessorResult):
            data = data.data
        
        content = data.content
        
        # Apply template if specified
        if self.config_obj.content_template:
            template = self.config_obj.content_template
            metadata = data.metadata
            
            # Format template with metadata
            formatted_content = template.format(
                content=content,
                title=metadata.get("title", "Untitled"),
                date=datetime.now().strftime("%Y-%m-%d"),
                time=datetime.now().strftime("%H-%M-%S"),
                id=data.id,
                source=data.source,
                **metadata
            )
            
            content = formatted_content
        
        # Add frontmatter if requested
        if self.config_obj.add_frontmatter:
            title = data.metadata.get("title", "Untitled")
            date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Prepare tags
            tags = self.config_obj.tags.copy()
            if "tags" in data.metadata and isinstance(data.metadata["tags"], list):
                tags.extend(data.metadata["tags"])
            
            # Create frontmatter
            frontmatter = "---\n"
            frontmatter += f"title: \"{title}\"\n"
            frontmatter += f"date: {date}\n"
            frontmatter += f"source: {data.source}\n"
            
            if tags:
                frontmatter += "tags:\n"
                for tag in tags:
                    frontmatter += f"  - {tag}\n"
            
            # Add extra metadata
            for key, value in data.metadata.items():
                if key != "title" and key != "tags" and isinstance(value, (str, int, float, bool)):
                    frontmatter += f"{key}: {value}\n"
            
            frontmatter += "---\n\n"
            
            # Only add frontmatter if content doesn't already have it
            if not content.startswith("---"):
                content = frontmatter + content
        
        # Add timestamp if requested
        if self.config_obj.add_timestamp and not self.config_obj.add_frontmatter:
            timestamp = f"> Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            # Add timestamp at the end if not already present
            if "Generated on " not in content:
                content += f"\n\n{timestamp}"
        
        return content
    
    @track_metrics
    def output(self, data: Union[PipelineData, ProcessorResult]) -> bool:
        """Output data to Obsidian markdown file.
        
        Args:
            data: The data to output
            
        Returns:
            True if output succeeded, False otherwise
        """
        try:
            file_path = self._get_file_path(data)
            content = self._format_content(data)
            
            # Check if file exists
            file_exists = os.path.exists(file_path)
            
            if file_exists:
                # Handle existing file
                if self.config_obj.append:
                    # Append to file
                    with open(file_path, "a", encoding="utf-8") as f:
                        f.write(f"\n\n{content}")
                    logger.info(f"Appended to file: {file_path}")
                    
                elif self.config_obj.prepend:
                    # Prepend to file
                    with open(file_path, "r", encoding="utf-8") as f:
                        existing_content = f.read()
                    
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(f"{content}\n\n{existing_content}")
                    logger.info(f"Prepended to file: {file_path}")
                    
                elif self.config_obj.overwrite:
                    # Overwrite file
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    logger.info(f"Overwrote file: {file_path}")
                    
                else:
                    # Default: create new file with unique name
                    file_name = os.path.basename(file_path)
                    base_name = os.path.splitext(file_name)[0]
                    dir_name = os.path.dirname(file_path)
                    
                    # Add timestamp to filename
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    new_path = os.path.join(dir_name, f"{base_name}_{timestamp}.md")
                    
                    with open(new_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    logger.info(f"Created new file: {new_path}")
                    
            else:
                # Create new file
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)
                logger.info(f"Created file: {file_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error outputting to Obsidian: {str(e)}")
            return False
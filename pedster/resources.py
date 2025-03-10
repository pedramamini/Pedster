"""Resource definitions for Pedster."""

import os
from typing import Dict, List

from dagster import ConfigurableResource, Field, InitResourceContext


class OpenAIResource(ConfigurableResource):
    """Resource for OpenAI and compatible API access."""
    
    api_key: str = Field(
        description="OpenAI API key. If not provided, will look for OPENAI_API_KEY env var.",
        default_value="",
    )
    api_base: str = Field(
        description="Base URL for the API.",
        default_value="https://api.openai.com/v1",
    )
    
    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Set up the resource for execution."""
        import openai
        
        # Get API key from environment if not provided
        api_key = self.api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OpenAI API key not found. Set OPENAI_API_KEY environment variable "
                "or provide api_key in configuration."
            )
        
        # Configure the client
        openai.api_key = api_key
        if self.api_base != "https://api.openai.com/v1":
            openai.api_base = self.api_base


class OpenRouterResource(ConfigurableResource):
    """Resource for OpenRouter API access."""
    
    api_key: str = Field(
        description="OpenRouter API key. If not provided, will look for OPENROUTER_API_KEY env var.",
        default_value="",
    )
    api_base: str = Field(
        description="Base URL for the API.",
        default_value="https://openrouter.ai/api/v1",
    )
    http_referer: str = Field(
        description="HTTP referer to identify your app.",
        default_value="https://pedster.ai",
    )
    models: Dict[str, str] = Field(
        description="Dict mapping model names to OpenRouter model IDs.",
        default_value={
            "gpt4o": "openai/gpt-4o",
            "claude": "anthropic/claude-3-7-sonnet",
            "o3mini": "mistralai/o3-mini",
        },
    )
    
    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Set up the resource for execution."""
        # Get API key from environment if not provided
        api_key = self.api_key or os.environ.get("OPENROUTER_API_KEY")
        if not api_key:
            context.log.warning(
                "OpenRouter API key not found. Set OPENROUTER_API_KEY environment variable "
                "or provide api_key in configuration."
            )


class ObsidianResource(ConfigurableResource):
    """Resource for Obsidian vault access."""
    
    vault_path: str = Field(
        description="Path to Obsidian vault directory.",
    )
    default_folder: str = Field(
        description="Default folder for new notes within the vault.",
        default_value="",
    )
    templates_folder: str = Field(
        description="Folder containing templates.",
        default_value="Templates",
    )
    default_tags: List[str] = Field(
        description="Default tags to apply to new notes.",
        default_value=["pedster"],
    )
    
    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Set up the resource for execution."""
        # Check if vault path exists
        if not os.path.exists(self.vault_path):
            raise ValueError(f"Obsidian vault path not found: {self.vault_path}")
        
        # Check if templates folder exists
        templates_path = os.path.join(self.vault_path, self.templates_folder)
        if self.templates_folder and not os.path.exists(templates_path):
            context.log.warning(f"Templates folder not found: {templates_path}")


class OllamaResource(ConfigurableResource):
    """Resource for Ollama local model access."""
    
    api_base: str = Field(
        description="Base URL for the Ollama API.",
        default_value="http://localhost:11434/api",
    )
    models: List[str] = Field(
        description="List of models to use.",
        default_value=["deepseek-r1:32b"],
    )
    
    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Set up the resource for execution."""
        import requests
        
        # Check if Ollama is running
        try:
            response = requests.get(
                f"{self.api_base.split('/api')[0]}/api/tags",
                timeout=2,
            )
            response.raise_for_status()
            
            # Check if models are available
            available_models = [model["name"] for model in response.json().get("models", [])]
            
            for model in self.models:
                if model not in available_models:
                    context.log.warning(
                        f"Model {model} not found in Ollama. "
                        f"Pull it with 'ollama pull {model}'"
                    )
            
        except Exception as e:
            context.log.error(f"Error connecting to Ollama: {str(e)}")
            context.log.warning(
                "Ollama doesn't seem to be running. Start it with 'ollama serve'"
            )
"""LLM processor using various models via OpenRouter."""

import os
import time
from abc import ABC
from typing import Any, Dict, List, Optional, Union

from dagster import Config, get_dagster_logger

from pedster.processors.base_processor import BaseProcessor
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData, ProcessorResult


logger = get_dagster_logger()


class LLMConfig(Config):
    """Base configuration for LLM processors."""
    
    api_base: str = "https://openrouter.ai/api/v1"
    api_key: Optional[str] = None
    model: str
    temperature: float = 0.7
    max_tokens: int = 1000
    top_p: float = 0.95
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    stop: Optional[List[str]] = None
    timeout: int = 60
    retry_count: int = 3
    retry_delay: int = 5


class PromptTemplate(Config):
    """Configuration for prompt templates."""
    
    template: str
    system_message: Optional[str] = None
    input_variables: List[str] = ["content"]
    output_format: str = "markdown"


class LLMProcessor(BaseProcessor, ABC):
    """Base processor for Large Language Models."""
    
    def __init__(
        self,
        name: str,
        description: str,
        model: str,
        prompt_template: Union[str, PromptTemplate],
        input_type: ContentType = ContentType.TEXT,
        output_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the LLM processor.
        
        Args:
            name: Processor name
            description: Processor description
            model: Model identifier
            prompt_template: Prompt template string or PromptTemplate object
            input_type: Type of content this processor accepts
            output_type: Type of content this processor produces
            config: Configuration dictionary
        """
        super().__init__(name, description, input_type, output_type, config)
        
        # Create config with model
        model_config = {"model": model}
        if config:
            model_config.update(config)
        
        # Set up configuration
        self.config_obj = LLMConfig(**model_config)
        
        # Set up prompt template
        if isinstance(prompt_template, str):
            self.prompt_template = PromptTemplate(template=prompt_template)
        else:
            self.prompt_template = prompt_template
    
    def _prepare_messages(self, content: str, metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
        """Prepare chat messages from content and prompt template.
        
        Args:
            content: Input content
            metadata: Optional metadata
            
        Returns:
            List of message dictionaries
        """
        # Prepare variables for template
        variables = {"content": content}
        if metadata:
            variables.update(metadata)
        
        # Format the prompt with variables
        prompt = self.prompt_template.template
        for var_name in self.prompt_template.input_variables:
            if var_name in variables:
                placeholder = f"{{{var_name}}}"
                prompt = prompt.replace(placeholder, str(variables[var_name]))
        
        # Prepare messages
        messages = []
        
        # Add system message if provided
        if self.prompt_template.system_message:
            messages.append({
                "role": "system",
                "content": self.prompt_template.system_message
            })
        
        # Add user message
        messages.append({
            "role": "user",
            "content": prompt
        })
        
        return messages
    
    @track_metrics
    def _call_openrouter(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """Call the OpenRouter API.
        
        Args:
            messages: List of message dictionaries
            
        Returns:
            API response
            
        Raises:
            Exception: If there's an error calling the API
        """
        import requests
        
        headers = {
            "Authorization": f"Bearer {self._get_api_key()}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://pedster.ai",  # Replace with your site URL
        }
        
        data = {
            "model": self.config_obj.model,
            "messages": messages,
            "temperature": self.config_obj.temperature,
            "max_tokens": self.config_obj.max_tokens,
            "top_p": self.config_obj.top_p,
            "frequency_penalty": self.config_obj.frequency_penalty,
            "presence_penalty": self.config_obj.presence_penalty,
        }
        
        if self.config_obj.stop:
            data["stop"] = self.config_obj.stop
        
        # Try multiple times with backoff
        for attempt in range(self.config_obj.retry_count):
            try:
                response = requests.post(
                    f"{self.config_obj.api_base}/chat/completions",
                    headers=headers,
                    json=data,
                    timeout=self.config_obj.timeout,
                )
                response.raise_for_status()
                return response.json()
                
            except requests.RequestException as e:
                logger.warning(f"API call failed (attempt {attempt + 1}): {str(e)}")
                
                if attempt < self.config_obj.retry_count - 1:
                    delay = self.config_obj.retry_delay * (attempt + 1)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise Exception(f"Failed to call API after {self.config_obj.retry_count} attempts: {str(e)}")
    
    def _get_api_key(self) -> str:
        """Get API key from config or environment.
        
        Returns:
            API key
            
        Raises:
            ValueError: If API key is not found
        """
        # Try config first
        if self.config_obj.api_key:
            return self.config_obj.api_key
        
        # Try environment variables
        api_key = os.environ.get("OPENROUTER_API_KEY")
        if api_key:
            return api_key
        
        raise ValueError(
            "API key not found. Set OPENROUTER_API_KEY environment variable "
            "or provide api_key in configuration."
        )
    
    @track_metrics
    def process(self, data: PipelineData) -> ProcessorResult:
        """Process data with the LLM.
        
        Args:
            data: The data to process
            
        Returns:
            ProcessorResult with model output
        """
        try:
            # Prepare messages
            messages = self._prepare_messages(data.content, data.metadata)
            logger.info(f"Calling {self.config_obj.model} with {len(messages)} messages")
            
            # Call API
            response = self._call_openrouter(messages)
            
            # Extract content
            if "choices" in response and len(response["choices"]) > 0:
                content = response["choices"][0]["message"]["content"]
                
                # Extract metrics
                usage = response.get("usage", {})
                tokens_in = usage.get("prompt_tokens", 0)
                tokens_out = usage.get("completion_tokens", 0)
                
                # Update metrics in data
                data_copy = data.model_copy(deep=True)
                data_copy.metrics.tokens_in = tokens_in
                data_copy.metrics.tokens_out = tokens_out
                
                # Add model info to metadata
                data_copy.metadata["model"] = self.config_obj.model
                data_copy.metadata["model_provider"] = response.get("model", "").split("/")[0]
                
                return self.create_result(
                    data_copy,
                    content=content,
                )
            else:
                logger.error(f"Unexpected API response format: {response}")
                return self.create_result(
                    data,
                    success=False,
                    error_message="Unexpected API response format",
                )
                
        except Exception as e:
            logger.error(f"Error calling LLM: {str(e)}")
            return self.create_result(
                data,
                success=False,
                error_message=f"LLM error: {str(e)}",
            )


class GPT4OProcessor(LLMProcessor):
    """Processor for OpenAI GPT-4o model."""
    
    def __init__(
        self,
        name: str = "gpt4o",
        description: str = "Process text with GPT-4o",
        prompt_template: Union[str, PromptTemplate] = "Please process the following content:\n\n{content}",
        input_type: ContentType = ContentType.TEXT,
        output_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the GPT-4o processor.
        
        Args:
            name: Processor name
            description: Processor description
            prompt_template: Prompt template
            input_type: Type of content this processor accepts
            output_type: Type of content this processor produces
            config: Configuration dictionary
        """
        super().__init__(
            name=name,
            description=description,
            model="openai/gpt-4o",
            prompt_template=prompt_template,
            input_type=input_type,
            output_type=output_type,
            config=config,
        )


class Claude37Processor(LLMProcessor):
    """Processor for Anthropic Claude 3.7 model."""
    
    def __init__(
        self,
        name: str = "claude",
        description: str = "Process text with Claude 3.7",
        prompt_template: Union[str, PromptTemplate] = "Please process the following content:\n\n{content}",
        input_type: ContentType = ContentType.TEXT,
        output_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the Claude 3.7 processor.
        
        Args:
            name: Processor name
            description: Processor description
            prompt_template: Prompt template
            input_type: Type of content this processor accepts
            output_type: Type of content this processor produces
            config: Configuration dictionary
        """
        super().__init__(
            name=name,
            description=description,
            model="anthropic/claude-3-7-sonnet",
            prompt_template=prompt_template,
            input_type=input_type,
            output_type=output_type,
            config=config,
        )


class OllamaDeepeekProcessor(LLMProcessor):
    """Processor for Ollama DeepSeek model."""
    
    def __init__(
        self,
        name: str = "deepseek",
        description: str = "Process text with DeepSeek-r1:32b via Ollama",
        prompt_template: Union[str, PromptTemplate] = "Please process the following content:\n\n{content}",
        input_type: ContentType = ContentType.TEXT,
        output_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the DeepSeek processor.
        
        Args:
            name: Processor name
            description: Processor description
            prompt_template: Prompt template
            input_type: Type of content this processor accepts
            output_type: Type of content this processor produces
            config: Configuration dictionary
        """
        # Override config to use Ollama API
        ollama_config = {
            "api_base": "http://localhost:11434/api",
            "temperature": 0.7,
            "max_tokens": 2000,
        }
        
        if config:
            ollama_config.update(config)
        
        super().__init__(
            name=name,
            description=description,
            model="deepseek-r1:32b",
            prompt_template=prompt_template,
            input_type=input_type,
            output_type=output_type,
            config=ollama_config,
        )
    
    @track_metrics
    def _call_openrouter(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        """Call the Ollama API instead of OpenRouter.
        
        Args:
            messages: List of message dictionaries
            
        Returns:
            API response in a format compatible with OpenRouter
            
        Raises:
            Exception: If there's an error calling the API
        """
        import requests
        
        headers = {
            "Content-Type": "application/json",
        }
        
        # Convert to Ollama format
        prompt = "\n".join([msg["content"] for msg in messages])
        
        data = {
            "model": self.config_obj.model,
            "prompt": prompt,
            "stream": False,
            "temperature": self.config_obj.temperature,
            "num_predict": self.config_obj.max_tokens,
        }
        
        # Try multiple times with backoff
        for attempt in range(self.config_obj.retry_count):
            try:
                response = requests.post(
                    f"{self.config_obj.api_base}/generate",
                    headers=headers,
                    json=data,
                    timeout=self.config_obj.timeout,
                )
                response.raise_for_status()
                
                # Convert Ollama response to OpenRouter format
                ollama_resp = response.json()
                
                # Calculate tokens (crude approximation)
                prompt_len = len(prompt.split())
                completion_len = len(ollama_resp.get("response", "").split())
                
                return {
                    "choices": [
                        {
                            "message": {
                                "content": ollama_resp.get("response", ""),
                                "role": "assistant",
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "model": f"ollama/{self.config_obj.model}",
                    "usage": {
                        "prompt_tokens": prompt_len,
                        "completion_tokens": completion_len,
                        "total_tokens": prompt_len + completion_len,
                    },
                }
                
            except requests.RequestException as e:
                logger.warning(f"Ollama API call failed (attempt {attempt + 1}): {str(e)}")
                
                if attempt < self.config_obj.retry_count - 1:
                    delay = self.config_obj.retry_delay * (attempt + 1)
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise Exception(f"Failed to call Ollama API after {self.config_obj.retry_count} attempts: {str(e)}")


class O3MiniProcessor(LLMProcessor):
    """Processor for O3-mini model via OpenRouter."""
    
    def __init__(
        self,
        name: str = "o3mini",
        description: str = "Process text with O3-mini",
        prompt_template: Union[str, PromptTemplate] = "Please process the following content:\n\n{content}",
        input_type: ContentType = ContentType.TEXT,
        output_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the O3-mini processor.
        
        Args:
            name: Processor name
            description: Processor description
            prompt_template: Prompt template
            input_type: Type of content this processor accepts
            output_type: Type of content this processor produces
            config: Configuration dictionary
        """
        super().__init__(
            name=name,
            description=description,
            model="mistralai/o3-mini",
            prompt_template=prompt_template,
            input_type=input_type,
            output_type=output_type,
            config=config,
        )
"""iMessage output using applescript."""

import subprocess
from typing import Any, Dict, List, Optional, Union

from dagster import Config, get_dagster_logger

from pedster.outputs.base_output import BaseOutput
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData, ProcessorResult


logger = get_dagster_logger()


class IMessageOutputConfig(Config):
    """Configuration for iMessage output."""
    
    recipients: List[str]
    max_length: int = 2000
    truncate_long_messages: bool = True
    split_long_messages: bool = False
    add_prefix: Optional[str] = None
    add_suffix: Optional[str] = None


class IMessageOutput(BaseOutput):
    """Output for sending iMessages."""
    
    def __init__(
        self,
        name: str = "imessage",
        description: str = "Send output as iMessage",
        accepted_types: Union[ContentType, List[ContentType]] = [ContentType.TEXT, ContentType.MARKDOWN],
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the iMessage output.
        
        Args:
            name: Output name
            description: Output description
            accepted_types: Type(s) of content this output accepts
            config: Configuration dictionary with recipients
        """
        super().__init__(name, description, accepted_types, config)
        
        if not config or "recipients" not in config:
            raise ValueError("recipients must be provided in config")
        
        self.config_obj = IMessageOutputConfig(**(config or {}))
    
    def _send_imessage(self, recipient: str, message: str) -> bool:
        """Send an iMessage using AppleScript.
        
        Args:
            recipient: The recipient (phone number or email)
            message: The message to send
            
        Returns:
            True if sending succeeded, False otherwise
        """
        try:
            # Escape double quotes in message
            escaped_message = message.replace('"', '\\"')
            
            # Create the AppleScript command
            applescript = f'''
                tell application "Messages"
                    send "{escaped_message}" to buddy "{recipient}" of service 1
                end tell
            '''
            
            # Run the AppleScript
            subprocess.run(
                ["osascript", "-e", applescript],
                check=True,
                capture_output=True,
                text=True,
            )
            
            logger.info(f"Sent iMessage to {recipient}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error sending iMessage to {recipient}: {e.stderr}")
            return False
            
        except Exception as e:
            logger.error(f"Error sending iMessage: {str(e)}")
            return False
    
    def _format_message(self, data: Union[PipelineData, ProcessorResult]) -> str:
        """Format content for sending as a message.
        
        Args:
            data: The data to format
            
        Returns:
            Formatted message
        """
        # Extract content data
        if isinstance(data, ProcessorResult):
            data = data.data
        
        content = data.content
        
        # Add prefix/suffix if configured
        if self.config_obj.add_prefix:
            content = f"{self.config_obj.add_prefix}\n\n{content}"
            
        if self.config_obj.add_suffix:
            content = f"{content}\n\n{self.config_obj.add_suffix}"
        
        # Handle long messages
        if len(content) > self.config_obj.max_length:
            if self.config_obj.truncate_long_messages:
                # Truncate and add indicator
                truncated_content = content[:self.config_obj.max_length - 20]
                content = f"{truncated_content}...\n[Message truncated]"
            elif not self.config_obj.split_long_messages:
                # Just log a warning but don't modify
                logger.warning(
                    f"Message length ({len(content)}) exceeds max_length "
                    f"({self.config_obj.max_length})"
                )
        
        return content
    
    @track_metrics
    def output(self, data: Union[PipelineData, ProcessorResult]) -> bool:
        """Send data as iMessage.
        
        Args:
            data: The data to output
            
        Returns:
            True if output succeeded, False otherwise
        """
        try:
            message = self._format_message(data)
            
            if self.config_obj.split_long_messages and len(message) > self.config_obj.max_length:
                # Split message into chunks
                chunks = []
                remaining = message
                chunk_size = self.config_obj.max_length
                
                while remaining:
                    if len(remaining) <= chunk_size:
                        chunks.append(remaining)
                        remaining = ""
                    else:
                        # Find a good breaking point
                        break_point = remaining[:chunk_size].rfind("\n\n")
                        if break_point == -1:
                            break_point = remaining[:chunk_size].rfind("\n")
                        if break_point == -1:
                            break_point = remaining[:chunk_size].rfind(". ")
                        if break_point == -1:
                            break_point = chunk_size
                        
                        chunks.append(remaining[:break_point])
                        remaining = remaining[break_point:].lstrip()
                
                # Send messages in chunks
                success = True
                for i, chunk in enumerate(chunks):
                    # Add part indicator
                    if len(chunks) > 1:
                        chunk = f"[Part {i+1}/{len(chunks)}]\n\n{chunk}"
                    
                    # Send to all recipients
                    for recipient in self.config_obj.recipients:
                        if not self._send_imessage(recipient, chunk):
                            success = False
                
                return success
                
            else:
                # Send single message to all recipients
                success = True
                for recipient in self.config_obj.recipients:
                    if not self._send_imessage(recipient, message):
                        success = False
                
                return success
                
        except Exception as e:
            logger.error(f"Error in iMessage output: {str(e)}")
            return False
"""Audio transcription processor using Whisper."""

import os
from typing import Any, Dict, List, Optional

from dagster import Config, get_dagster_logger

from pedster.processors.base_processor import BaseProcessor
from pedster.utils.metrics import track_metrics
from pedster.utils.models import ContentType, PipelineData, ProcessorResult


logger = get_dagster_logger()


class TranscriptionConfig(Config):
    """Configuration for transcription processor."""
    
    model_size: str = "base"  # tiny, base, small, medium, large
    language: Optional[str] = None
    task: str = "transcribe"  # transcribe or translate
    device: str = "cpu"  # cpu or cuda
    fp16: bool = False
    temperature: float = 0.0
    beam_size: int = 5
    patience: float = 1.0
    use_timestamps: bool = True
    output_format: str = "markdown"  # text, json, markdown


class TranscriptionProcessor(BaseProcessor):
    """Processor for audio transcription using Whisper."""
    
    def __init__(
        self,
        name: str = "transcription",
        description: str = "Audio transcription using Whisper",
        input_type: ContentType = ContentType.AUDIO,
        output_type: ContentType = ContentType.TEXT,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the transcription processor.
        
        Args:
            name: Processor name
            description: Processor description
            input_type: Type of content this processor accepts
            output_type: Type of content this processor produces
            config: Configuration dictionary
        """
        super().__init__(name, description, input_type, output_type, config)
        self.config_obj = TranscriptionConfig(**(config or {}))
        self.model = None
    
    def _load_model(self) -> Any:
        """Load the Whisper model.
        
        Returns:
            Loaded model
            
        Raises:
            ImportError: If whisper is not installed
        """
        try:
            import whisper
            
            if self.model is None:
                logger.info(f"Loading Whisper {self.config_obj.model_size} model...")
                self.model = whisper.load_model(
                    self.config_obj.model_size,
                    device=self.config_obj.device,
                )
                logger.info("Model loaded successfully")
            
            return self.model
            
        except ImportError:
            raise ImportError(
                "Whisper is not installed. Install it with 'pip install whisper'"
            )
    
    @track_metrics
    def _transcribe_audio(self, audio_path: str) -> Dict[str, Any]:
        """Transcribe audio file using Whisper.
        
        Args:
            audio_path: Path to audio file
            
        Returns:
            Dictionary with transcription results
            
        Raises:
            FileNotFoundError: If the audio file doesn't exist
            Exception: If there's an error during transcription
        """
        if not os.path.exists(audio_path):
            raise FileNotFoundError(f"Audio file not found: {audio_path}")
        
        model = self._load_model()
        
        # Transcribe audio
        result = model.transcribe(
            audio_path,
            language=self.config_obj.language,
            task=self.config_obj.task,
            temperature=self.config_obj.temperature,
            beam_size=self.config_obj.beam_size,
            patience=self.config_obj.patience,
            fp16=self.config_obj.fp16,
        )
        
        return result
    
    def _format_output(self, result: Dict[str, Any]) -> str:
        """Format transcription result based on output format.
        
        Args:
            result: Transcription result from Whisper
            
        Returns:
            Formatted transcription
        """
        if self.config_obj.output_format == "json":
            import json
            return json.dumps(result, indent=2)
            
        elif self.config_obj.output_format == "markdown":
            text = result["text"]
            
            if self.config_obj.use_timestamps and "segments" in result:
                formatted_text = "# Transcription\n\n"
                
                for segment in result["segments"]:
                    start = segment["start"]
                    end = segment["end"]
                    segment_text = segment["text"].strip()
                    
                    # Format timestamp as [MM:SS]
                    start_fmt = f"{int(start // 60):02d}:{int(start % 60):02d}"
                    end_fmt = f"{int(end // 60):02d}:{int(end % 60):02d}"
                    
                    formatted_text += f"**[{start_fmt} - {end_fmt}]** {segment_text}\n\n"
                
                return formatted_text
            else:
                return f"# Transcription\n\n{text}"
        
        # Default to plain text
        return result["text"]
    
    @track_metrics
    def process(self, data: PipelineData) -> ProcessorResult:
        """Process audio data for transcription.
        
        Args:
            data: The audio data to process
            
        Returns:
            ProcessorResult with transcription
            
        Raises:
            ValueError: If content is not a valid audio path
        """
        # Check if the content is an audio file path
        if not isinstance(data.content, str):
            return self.create_result(
                data,
                success=False,
                error_message="Content must be a string path to an audio file",
            )
        
        try:
            # Transcribe audio
            audio_path = data.content
            logger.info(f"Transcribing audio file: {audio_path}")
            
            result = self._transcribe_audio(audio_path)
            
            # Format output
            transcription = self._format_output(result)
            
            # Create metadata
            metadata = {
                **data.metadata,
                "language": result.get("language"),
                "duration": result.get("duration"),
                "segments": len(result.get("segments", [])),
            }
            
            # Update metadata in a copy of the data
            data_copy = data.model_copy(deep=True)
            data_copy.metadata = metadata
            
            # Return result
            return self.create_result(
                data_copy,
                content=transcription,
                content_type=ContentType.TEXT,
            )
            
        except Exception as e:
            logger.error(f"Error transcribing audio: {str(e)}")
            return self.create_result(
                data,
                success=False,
                error_message=f"Transcription error: {str(e)}",
            )
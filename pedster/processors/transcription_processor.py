"""Audio transcription processor using Whisper with domain expertise correction."""

import os
import json
import requests
from typing import Any, Dict, List, Optional, Tuple

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
    correct_with_domain_expertise: bool = True  # Correct transcripts using domain expertise
    openrouter_api_key: Optional[str] = None  # OpenRouter API key for domain detection & correction
    openrouter_model: str = "anthropic/claude-3-5-sonnet"  # Model to use for domain detection & correction
    topic_sample_size: int = 4000  # Number of characters to sample for topic detection


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
    def _detect_domain(self, title: str, transcript_sample: str) -> str:
        """Detect the domain expertise needed for this transcript.
        
        Args:
            title: The title of the content
            transcript_sample: Sample text from the transcript
            
        Returns:
            Domain expertise string (e.g., "Finance", "Machine Learning", etc.)
        """
        if not self.config_obj.openrouter_api_key:
            logger.warning("No OpenRouter API key provided for domain detection")
            return "General"
            
        logger.info("Detecting domain expertise for transcript correction")
        
        # Prepare prompt for domain detection
        prompt = f"""Given the following podcast title and transcript sample, determine the specific professional or technical domain this content belongs to. Focus on identifying specialized fields that might have unique terminology (e.g., Brazilian Jiu-Jitsu, Quantum Physics, Constitutional Law, etc.).

Title: {title}

Transcript Sample:
{transcript_sample}

Respond with just the domain name, nothing else."""

        # Call OpenRouter API
        headers = {
            "Authorization": f"Bearer {self.config_obj.openrouter_api_key}",
            "HTTP-Referer": "https://github.com/pedster/pedster",
        }
        
        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json={
                    "model": self.config_obj.openrouter_model,
                    "messages": [{
                        "role": "user",
                        "content": prompt
                    }]
                }
            )
            response.raise_for_status()
            response_data = response.json()
            
            domain = response_data["choices"][0]["message"]["content"].strip()
            logger.info(f"Detected domain expertise: {domain}")
            return domain
            
        except Exception as e:
            logger.error(f"Error detecting domain: {str(e)}")
            return "General"
    
    @track_metrics
    def _correct_transcript(self, transcript: str, domain: str) -> Tuple[str, Dict[str, Any]]:
        """Correct the transcript using domain-specific expertise.
        
        Args:
            transcript: The raw transcript from Whisper
            domain: The detected domain expertise
            
        Returns:
            Tuple of (corrected transcript, correction metadata)
        """
        if not self.config_obj.openrouter_api_key:
            logger.warning("No OpenRouter API key provided for transcript correction")
            return transcript, {"corrections": "No corrections made - missing API key"}
            
        logger.info(f"Correcting transcript with domain expertise: {domain}")
        
        # Prepare prompt for domain-specific correction
        prompt = f"""You are a professional transcriptionist with extensive expertise in {domain}. Your task is to correct any technical terms, jargon, or domain-specific language in this transcript that might have been misinterpreted during speech-to-text conversion.

Focus on:
1. Technical terminology specific to {domain}
2. Names of key figures or concepts in the field
3. Specialized vocabulary and acronyms
4. Common terms that might have been confused with domain-specific ones

Transcript:
{transcript}

Provide your response in the following format:

CORRECTED TRANSCRIPT:
[Your corrected transcript here, maintaining all original formatting and structure]

CHANGES MADE:
- List each significant correction you made
- Include the original text and what you changed it to
- If no changes were needed, state that"""

        # Call OpenRouter API
        headers = {
            "Authorization": f"Bearer {self.config_obj.openrouter_api_key}",
            "HTTP-Referer": "https://github.com/pedster/pedster",
        }
        
        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json={
                    "model": self.config_obj.openrouter_model,
                    "messages": [{
                        "role": "user",
                        "content": prompt
                    }]
                }
            )
            response.raise_for_status()
            response_data = response.json()
            
            content = response_data["choices"][0]["message"]["content"].strip()
            
            # Parse response
            parts = content.split("\nCHANGES MADE:")
            if len(parts) == 2:
                corrected_transcript = parts[0].replace("CORRECTED TRANSCRIPT:\n", "").strip()
                changes = parts[1].strip()
                logger.info(f"Transcript corrected with changes: {changes[:200]}...")
                return corrected_transcript, {"domain": domain, "corrections": changes}
            else:
                logger.warning("Unexpected format in transcript correction response")
                return transcript, {"domain": domain, "corrections": "Error in correction format"}
            
        except Exception as e:
            logger.error(f"Error correcting transcript: {str(e)}")
            return transcript, {"domain": domain, "corrections": f"Error: {str(e)}"}
    
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
            
            # Get title from metadata if available
            title = data.metadata.get("title", "Untitled Content")
            
            # Perform base transcription with Whisper
            result = self._transcribe_audio(audio_path)
            raw_transcript = result["text"]
            
            # Correction process
            if (self.config_obj.correct_with_domain_expertise and 
                self.config_obj.openrouter_api_key and 
                raw_transcript):
                
                # Take a sample for domain detection (to save tokens)
                sample_size = min(len(raw_transcript), self.config_obj.topic_sample_size)
                transcript_sample = raw_transcript[:sample_size]
                
                # Detect domain and correct transcript
                domain = self._detect_domain(title, transcript_sample)
                corrected_transcript, correction_metadata = self._correct_transcript(raw_transcript, domain)
                
                # Use the corrected transcript
                result["text"] = corrected_transcript
                corrections_info = correction_metadata
            else:
                corrections_info = {"corrections": "No domain correction applied"}
            
            # Format output
            transcription = self._format_output(result)
            
            # Create metadata
            metadata = {
                **data.metadata,
                "language": result.get("language"),
                "duration": result.get("duration"),
                "segments": len(result.get("segments", [])),
                **corrections_info,
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
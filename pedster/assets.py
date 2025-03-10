"""Asset definitions for Pedster."""

from typing import Dict, List

from dagster import AssetIn, AssetKey, asset

from pedster.ingestors.cli_ingestor import CLIIngestor
from pedster.ingestors.imessage_ingestor import IMessageIngestor
from pedster.ingestors.rss_ingestor import RSSIngestor
from pedster.ingestors.web_ingestor import WebIngestor
from pedster.outputs.imessage_output import IMessageOutput
from pedster.outputs.obsidian_output import ObsidianOutput
from pedster.processors.llm_processor import (Claude37Processor, GPT4OProcessor,
                                              O3MiniProcessor,
                                              OllamaDeepeekProcessor,
                                              PromptTemplate)
from pedster.processors.map_reduce_processor import MapReduceProcessor
from pedster.processors.transcription_processor import TranscriptionProcessor
from pedster.resources import (ObsidianResource, OllamaResource,
                               OpenRouterResource)
from pedster.utils.models import ContentType, PipelineData, ProcessorResult


# Define standard prompts
SUMMARIZE_PROMPT = PromptTemplate(
    template="Please summarize the following content in a clear and concise manner:\n\n{content}",
    system_message="You are a helpful AI assistant that summarizes content accurately while preserving key information.",
)

ANALYZE_PROMPT = PromptTemplate(
    template="Please analyze the following content, highlighting key points, patterns, and insights:\n\n{content}",
    system_message="You are an analytical AI assistant that provides insightful analysis of content.",
)

FORMAT_MARKDOWN_PROMPT = PromptTemplate(
    template="Please convert the following content into well-formatted Markdown suitable for Obsidian notes:\n\n{content}",
    system_message="You are an AI assistant specialized in converting content to clean, well-structured Markdown for note-taking applications.",
)


# Ingestor assets
@asset(group="ingestors")
def cli_ingestor() -> CLIIngestor:
    """Command line ingestor asset factory."""
    return CLIIngestor(
        name="cli",
        description="Command line ingestor",
        source_name="cli",
        content_type=ContentType.TEXT,
    )


@asset(group="ingestors")
def imessage_ingestor() -> IMessageIngestor:
    """iMessage ingestor asset factory."""
    return IMessageIngestor(
        name="imessage",
        description="iMessage ingestor",
        source_name="imessage",
        content_type=ContentType.TEXT,
        config={
            "trigger_word": "pedster",
            "lookback_hours": 24,
        },
    )


@asset(group="ingestors")
def rss_ingestor(feed_urls: List[str]) -> RSSIngestor:
    """RSS ingestor asset factory."""
    return RSSIngestor(
        name="rss",
        description="RSS feed ingestor",
        source_name="rss",
        content_type=ContentType.TEXT,
        config={
            "feed_urls": feed_urls,
            "max_entries_per_feed": 5,
        },
    )


@asset(group="ingestors")
def web_ingestor(urls: List[str]) -> WebIngestor:
    """Web ingestor asset factory."""
    return WebIngestor(
        name="web",
        description="Web ingestor using Jina",
        source_name="web",
        content_type=ContentType.TEXT,
        config={
            "urls": urls,
            "extract_text": True,
        },
    )


# Processor assets
@asset(group="processors")
def transcription_processor() -> TranscriptionProcessor:
    """Transcription processor asset factory."""
    return TranscriptionProcessor(
        name="transcription",
        description="Audio transcription processor",
        input_type=ContentType.AUDIO,
        output_type=ContentType.TEXT,
        config={
            "model_size": "base",
            "output_format": "markdown",
        },
    )


@asset(group="processors", required_resource_keys={"openrouter"})
def gpt4o_processor(openrouter: OpenRouterResource) -> GPT4OProcessor:
    """GPT-4o processor asset factory."""
    return GPT4OProcessor(
        name="gpt4o",
        description="OpenAI GPT-4o processor",
        prompt_template=FORMAT_MARKDOWN_PROMPT,
        config={
            "api_key": openrouter.api_key,
            "api_base": openrouter.api_base,
            "model": openrouter.models.get("gpt4o", "openai/gpt-4o"),
        },
    )


@asset(group="processors", required_resource_keys={"openrouter"})
def claude_processor(openrouter: OpenRouterResource) -> Claude37Processor:
    """Claude 3.7 processor asset factory."""
    return Claude37Processor(
        name="claude",
        description="Anthropic Claude 3.7 processor",
        prompt_template=ANALYZE_PROMPT,
        config={
            "api_key": openrouter.api_key,
            "api_base": openrouter.api_base,
            "model": openrouter.models.get("claude", "anthropic/claude-3-7-sonnet"),
        },
    )


@asset(group="processors", required_resource_keys={"openrouter"})
def o3mini_processor(openrouter: OpenRouterResource) -> O3MiniProcessor:
    """O3-mini processor asset factory."""
    return O3MiniProcessor(
        name="o3mini",
        description="Mistral O3-mini processor",
        prompt_template=SUMMARIZE_PROMPT,
        config={
            "api_key": openrouter.api_key,
            "api_base": openrouter.api_base,
            "model": openrouter.models.get("o3mini", "mistralai/o3-mini"),
        },
    )


@asset(group="processors", required_resource_keys={"ollama"})
def deepseek_processor(ollama: OllamaResource) -> OllamaDeepeekProcessor:
    """DeepSeek processor asset factory."""
    return OllamaDeepeekProcessor(
        name="deepseek",
        description="DeepSeek-r1:32b processor via Ollama",
        prompt_template=FORMAT_MARKDOWN_PROMPT,
        config={
            "api_base": ollama.api_base,
            "model": ollama.models[0] if ollama.models else "deepseek-r1:32b",
        },
    )


@asset(group="processors")
def map_reduce_processor(
    gpt4o_processor: GPT4OProcessor,
    claude_processor: Claude37Processor,
    o3mini_processor: O3MiniProcessor,
) -> MapReduceProcessor:
    """Map-reduce processor asset factory."""
    return MapReduceProcessor(
        name="model_compare",
        description="Compare results from multiple models",
        processors=[gpt4o_processor, claude_processor, o3mini_processor],
        config={
            "max_workers": 3,
            "parallel": True,
            "combine_results": True,
        },
    )


# Output assets
@asset(group="outputs", required_resource_keys={"obsidian"})
def obsidian_output(obsidian: ObsidianResource) -> ObsidianOutput:
    """Obsidian output asset factory."""
    return ObsidianOutput(
        name="obsidian",
        description="Output to Obsidian markdown files",
        config={
            "vault_path": obsidian.vault_path,
            "folder": obsidian.default_folder,
            "tags": obsidian.default_tags,
            "add_frontmatter": True,
        },
    )


@asset(group="outputs")
def imessage_output(recipients: List[str]) -> IMessageOutput:
    """iMessage output asset factory."""
    return IMessageOutput(
        name="imessage",
        description="Send output as iMessage",
        config={
            "recipients": recipients,
            "max_length": 2000,
            "split_long_messages": True,
        },
    )


# Data processing assets
@asset(
    ins={"cli_data": AssetIn("cli_data")},
    group="data",
)
def cli_to_model(
    cli_data: List[PipelineData], gpt4o_processor: GPT4OProcessor
) -> List[ProcessorResult]:
    """Process CLI data with GPT-4o."""
    results = []
    for data in cli_data:
        result = gpt4o_processor.process(data)
        results.append(result)
    return results


@asset(
    ins={"model_results": AssetIn("cli_to_model")},
    group="data",
)
def model_to_obsidian(
    model_results: List[ProcessorResult], obsidian_output: ObsidianOutput
) -> List[bool]:
    """Output model results to Obsidian."""
    results = []
    for result in model_results:
        output_result = obsidian_output.output(result)
        results.append(output_result)
    return results


@asset(
    ins={"rss_data": AssetIn("rss_data")},
    group="data",
)
def rss_to_models(
    rss_data: List[PipelineData], map_reduce_processor: MapReduceProcessor
) -> List[ProcessorResult]:
    """Process RSS data with multiple models."""
    results = []
    for data in rss_data:
        result = map_reduce_processor.process(data)
        results.append(result)
    return results


@asset(
    ins={"model_results": AssetIn("rss_to_models")},
    group="data",
)
def models_to_obsidian(
    model_results: List[ProcessorResult], obsidian_output: ObsidianOutput
) -> List[bool]:
    """Output model comparison results to Obsidian."""
    results = []
    for result in model_results:
        output_result = obsidian_output.output(result)
        results.append(output_result)
    return results
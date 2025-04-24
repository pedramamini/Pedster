"""Dagster definitions for Pedster."""

from dagster import (Definitions, EnvVar, Field, List, MultiPartitionsDefinition,
                    ResourceDefinition, ScheduleDefinition, SourceAsset,
                    StaticPartitionsDefinition, StringSource, TimeWindowPartitionsDefinition,
                    asset, define_asset_job, job, schedule)

import pedster.assets as assets
from pedster.resources import ObsidianResource, OllamaResource, OpenRouterResource


# Define resources
resources = {
    "openrouter": OpenRouterResource(
        api_key=EnvVar("OPENROUTER_API_KEY"),
    ),
    "obsidian": ObsidianResource(
        vault_path=Field(
            StringSource,
            description="Path to Obsidian vault directory",
            default_value="/Users/pedram/Documents/Obsidian/Main Vault",
        ),
        default_folder="Pedster",
        default_tags=["pedster", "ai"],
    ),
    "ollama": OllamaResource(),
}

# Define all assets
all_assets = [
    # Ingestors
    assets.cli_ingestor,
    assets.imessage_ingestor,
    assets.rss_ingestor,
    assets.podcast_ingestor,
    assets.web_ingestor,
    
    # Processors
    assets.transcription_processor,
    assets.gpt4o_processor,
    assets.claude_processor,
    assets.o3mini_processor, 
    assets.deepseek_processor,
    assets.map_reduce_processor,
    
    # Outputs
    assets.obsidian_output,
    assets.imessage_output,
    
    # Data flows
    assets.cli_to_model,
    assets.model_to_obsidian,
    assets.rss_to_models,
    assets.models_to_obsidian,
    assets.podcast_to_transcript,
    assets.transcripts_to_summary,
    assets.podcast_to_obsidian,
]

# Define jobs
cli_job = define_asset_job(
    name="cli_to_obsidian",
    selection=["cli_ingestor", "cli_data", "cli_to_model", "model_to_obsidian"],
    config={
        "resources": {
            "openrouter": {"config": {"api_key": {"env": "OPENROUTER_API_KEY"}}},
            "obsidian": {"config": {"vault_path": "/Users/pedram/Documents/Obsidian/Main Vault"}},
        }
    },
)

rss_job = define_asset_job(
    name="rss_to_obsidian",
    selection=["rss_ingestor", "rss_data", "rss_to_models", "models_to_obsidian"],
    config={
        "resources": {
            "openrouter": {"config": {"api_key": {"env": "OPENROUTER_API_KEY"}}},
            "obsidian": {"config": {"vault_path": "/Users/pedram/Documents/Obsidian/Main Vault"}},
        },
        "ops": {
            "rss_ingestor": {
                "config": {
                    "feed_urls": [
                        "https://news.ycombinator.com/rss",
                        "https://www.theverge.com/rss/index.xml",
                    ]
                }
            }
        }
    },
)

# Define podcast job
podcast_job = define_asset_job(
    name="podcast_to_obsidian",
    selection=[
        "podcast_ingestor", 
        "podcast_data", 
        "podcast_to_transcript", 
        "transcripts_to_summary",
        "podcast_to_obsidian"
    ],
    config={
        "resources": {
            "openrouter": {"config": {"api_key": {"env": "OPENROUTER_API_KEY"}}},
            "obsidian": {"config": {"vault_path": "/Users/pedram/Documents/Obsidian/Main Vault"}},
        },
        "ops": {
            "podcast_ingestor": {
                "config": {
                    "feed_urls": [
                        # Add your podcast feed URLs here
                    ]
                }
            }
        }
    },
)

# Define schedules
rss_schedule = ScheduleDefinition(
    name="hourly_rss_update",
    job=rss_job,
    cron_schedule="0 * * * *",  # Run every hour
)

imessage_schedule = ScheduleDefinition(
    name="imessage_check",
    job=define_asset_job(
        name="imessage_check_job",
        selection=["imessage_ingestor", "imessage_data"],
    ),
    cron_schedule="*/10 * * * *",  # Run every 10 minutes
)

podcast_schedule = ScheduleDefinition(
    name="daily_podcast_update",
    job=podcast_job,
    cron_schedule="0 8 * * *",  # Run every day at 8am
)

# Define Dagster definitions
defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[cli_job, rss_job, podcast_job],
    schedules=[rss_schedule, imessage_schedule, podcast_schedule],
)
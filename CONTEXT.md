# Pedster Project Context

## Current Task
Integrating RSS and podcast functionality from rssidian and podsidian into Pedster.

## Progress So Far
1. Created database models for feeds, articles, podcasts, episodes, and messages
2. Implemented enhanced RSS ingestor with Jina.ai content extraction
3. Implemented podcast ingestor with Whisper transcription
4. Enhanced transcription processor with domain expertise detection
5. Updated iMessage ingestor with better database functionality
6. Created Dagster assets, jobs, and pipelines for all components
7. Updated configuration files and dependencies

## Key Components Added
- SQLite database models for persistent storage
- RSS feed ingestion with full content extraction
- Podcast ingestion with audio download and transcription
- Domain expertise detection and transcript correction
- iMessage database access

## References 
- Used code from rssidian and podsidian in the references folder
- Integrated the transcript correction logic from podsidian's core.py
- Applied the database model pattern from both reference projects

## Next Steps
1. Consider implementing vector embedding functionality for content search
2. Add web ingestor functionality from ingest-self-texts.py
3. Implement more test cases
4. Consider additional outputs beyond Obsidian
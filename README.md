# Pedster

A modular data processing pipeline built with Dagster, designed to ingest, process, and output data from various sources.

## Features

- **Multiple Ingestors**:
  - Command line (stdin)
  - Web pages via Jina
  - RSS/Atom feeds
  - iMessage integration (macOS)
  - Podcast integration (coming soon)

- **Processors**:
  - Audio transcription via Whisper
  - LLM processing with multiple models:
    - OpenAI GPT-4o (via OpenRouter)
    - Anthropic Claude 3.7 (via OpenRouter)
    - Mistral O3-mini (via OpenRouter)
    - DeepSeek-r1:32b (via Ollama)
  - Map/Reduce pattern for parallel model processing

- **Outputs**:
  - Beautiful Markdown for Obsidian
  - iMessage sending

## Setup & Installation

### Quick Setup

For a quick and easy setup, use the provided initialization script:

```bash
# Clone the repository
git clone https://github.com/yourusername/pedster.git
cd pedster

# Run the initialization script
./scripts/init.sh
```

This script will:
- Create and activate a virtual environment
- Install the package in development mode
- Set up the configuration file in `~/.config/pedster/config.toml`
- Run a basic test to verify the installation

### Manual Setup

If you prefer to set up manually:

```bash
# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the package in development mode
pip install -e ".[dev]"

# Set up configuration
cp config.toml.example ~/.config/pedster/config.toml
# Edit the config file with your settings

# Load environment variables from config
eval $(python scripts/setup_env.py)
```

## Configuration

Pedster uses a TOML configuration file located at `~/.config/pedster/config.toml`. You can modify this file or create a new one:

```toml
[api]
openrouter_key = "your-api-key"  # OpenRouter API key

[obsidian]
vault_path = "/path/to/your/obsidian/vault"
default_folder = "Pedster"  # Default folder within vault
tags = ["pedster", "ai"]  # Default tags for notes

[rss]
feed_urls = ["https://news.ycombinator.com/rss"]
```

Load the configuration as environment variables:

```bash
eval $(python scripts/setup_env.py)
# Or with a custom config file:
eval $(python scripts/setup_env.py --config /path/to/custom/config.toml)
```

## Running Tests

```bash
# Run all tests
python scripts/run_tests.py

# Run with coverage report
python scripts/run_tests.py --coverage

# Run specific tests
python scripts/run_tests.py --pattern test_cli_ingestor.py
```

## Running the Dagster UI

```bash
# Make sure your environment variables are loaded
eval $(python scripts/setup_env.py)

# Start the Dagster UI
dagster dev
```

Visit http://localhost:3000 to access the Dagster UI.

## Usage Examples

### Process text from stdin with an LLM

```bash
echo "Summarize this article: https://example.com/article" | python -m pedster process --model gpt4o
```

### Run a specific job

```bash
# Process RSS feeds and save to Obsidian
dagster job execute -j rss_to_obsidian
```

## Project Structure

```
pedster/
├── pedster/                 # Main package
│   ├── ingestors/           # Data ingestors (CLI, RSS, web, etc.)
│   ├── processors/          # Data processors (LLMs, transcription, etc.)
│   ├── outputs/             # Output handlers (Obsidian, iMessage, etc.)
│   ├── utils/               # Utility functions and models
│   ├── assets.py            # Dagster asset definitions
│   ├── definitions.py       # Dagster pipeline definitions
│   └── resources.py         # Dagster resource definitions
├── tests/                   # Test directory
├── scripts/                 # Utility scripts
│   ├── init.sh              # Project initialization script
│   ├── cleanup.sh           # Cleanup script
│   ├── run_tests.py         # Test runner script
│   └── setup_env.py         # Environment setup script
├── config.toml.example      # Example configuration file
└── README.md                # This README
```

## Architecture

Pedster is built around three core concepts:

1. **Ingestors**: Components that bring data into the pipeline
2. **Processors**: Components that transform data
3. **Outputs**: Components that send data to its destination

All components pass around a standardized JSON format, making it easy to connect different parts of the pipeline.

## Adding New Components

### Creating a new ingestor

Inherit from `BaseIngestor` and implement the `ingest()` method:

```python
from pedster.ingestors.base_ingestor import BaseIngestor
from pedster.utils.models import ContentType, PipelineData

class MyIngestor(BaseIngestor):
    def ingest(self) -> List[PipelineData]:
        # Fetch data from somewhere
        data = fetch_my_data()
        
        # Return as pipeline data
        return [self.create_pipeline_data(data)]
```

### Creating a new processor

Inherit from `BaseProcessor` and implement the `process()` method:

```python
from pedster.processors.base_processor import BaseProcessor
from pedster.utils.models import PipelineData, ProcessorResult

class MyProcessor(BaseProcessor):
    def process(self, data: PipelineData) -> ProcessorResult:
        # Process the data
        processed = transform_data(data.content)
        
        # Return result
        return self.create_result(data, content=processed)
```

### Creating a new output

Inherit from `BaseOutput` and implement the `output()` method:

```python
from pedster.outputs.base_output import BaseOutput
from pedster.utils.models import PipelineData, ProcessorResult

class MyOutput(BaseOutput):
    def output(self, data: Union[PipelineData, ProcessorResult]) -> bool:
        # Extract content
        if isinstance(data, ProcessorResult):
            data = data.data
        
        # Send data somewhere
        success = send_my_data(data.content)
        
        return success
```

## Cleanup

To clean up temporary files and caches:

```bash
# Basic cleanup (removes Python cache files and build artifacts)
./scripts/cleanup.sh

# Remove virtual environment as well
./scripts/cleanup.sh --remove-venv

# Remove configuration file as well
./scripts/cleanup.sh --remove-config

# Remove everything (virtual env and config)
./scripts/cleanup.sh --all
```

## License

MIT
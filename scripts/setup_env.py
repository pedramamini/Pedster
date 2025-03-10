#!/usr/bin/env python3
"""
Setup environment variables from TOML configuration.

Usage:
    python setup_env.py [--config CONFIG_FILE]

This script reads a TOML configuration file and exports environment variables,
which can be used by the Pedster application.

Example:
    # Export variables for the current shell session
    eval $(python setup_env.py)
    
    # Use a custom config file
    eval $(python setup_env.py --config ./my_config.toml)
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Any

try:
    import tomli
except ImportError:
    try:
        import tomllib as tomli
    except ImportError:
        print("Error: tomli or tomllib package is required. Install with: pip install tomli")
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Setup environment from TOML config")
    parser.add_argument(
        "--config", "-c",
        type=str,
        default=str(Path.home() / ".config" / "pedster" / "config.toml"),
        help="Path to TOML configuration file",
    )
    return parser.parse_args()


def read_toml_config(config_path: str) -> Dict[str, Any]:
    """Read TOML configuration file."""
    try:
        with open(config_path, "rb") as f:
            return tomli.load(f)
    except FileNotFoundError:
        print(f"Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    except tomli.TOMLDecodeError as e:
        print(f"Error parsing TOML: {e}", file=sys.stderr)
        sys.exit(1)


def flatten_config(config: Dict[str, Any], prefix: str = "") -> Dict[str, str]:
    """Flatten nested TOML config into environment variables format."""
    env_vars = {}
    
    for key, value in config.items():
        env_key = f"{prefix}{key}".upper()
        
        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            nested_vars = flatten_config(value, f"{env_key}_")
            env_vars.update(nested_vars)
        elif isinstance(value, (list, tuple)):
            # Convert lists to comma-separated values
            env_vars[env_key] = ",".join(str(item) for item in value)
        elif value is not None:
            # Convert other values to strings
            env_vars[env_key] = str(value)
    
    return env_vars


def print_env_exports(env_vars: Dict[str, str]) -> None:
    """Print environment variable export commands for shell evaluation."""
    for key, value in env_vars.items():
        # Escape single quotes in the value
        escaped_value = value.replace("'", "'\\''")
        print(f"export {key}='{escaped_value}';")


def main() -> None:
    """Main function."""
    args = parse_args()
    config = read_toml_config(args.config)
    
    # Create default config if it doesn't exist
    if not os.path.exists(args.config) and args.config == str(Path.home() / ".config" / "pedster" / "config.toml"):
        os.makedirs(os.path.dirname(args.config), exist_ok=True)
        with open(args.config, "w") as f:
            f.write("""# Pedster Configuration

[api]
openrouter_key = ""  # Your OpenRouter API key

[obsidian]
vault_path = ""  # Path to your Obsidian vault
default_folder = "Pedster"  # Default folder within vault
tags = ["pedster", "ai"]  # Default tags for notes

[rss]
feed_urls = [
    "https://news.ycombinator.com/rss",
    "https://www.theverge.com/rss/index.xml"
]

[imessage]
recipients = []  # List of phone numbers or emails for sending messages
trigger_word = "pedster"  # Word that triggers processing
lookback_hours = 24  # How far back to look for messages

[ollama]
models = ["deepseek-r1:32b"]  # Models to use with Ollama
""")
        print(f"Created default config at {args.config}", file=sys.stderr)
    
    env_vars = flatten_config(config)
    print_env_exports(env_vars)


if __name__ == "__main__":
    main()
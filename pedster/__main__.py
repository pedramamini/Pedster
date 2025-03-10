"""CLI entry point for Pedster."""

import argparse
import sys
from typing import List, Optional

from pedster.ingestors.cli_ingestor import CLIIngestor
from pedster.processors.llm_processor import GPT4OProcessor, PromptTemplate
from pedster.utils.models import ContentType


def main(args: Optional[List[str]] = None) -> None:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Pedster - Data processing pipeline")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Process command
    process_parser = subparsers.add_parser("process", help="Process input text")
    process_parser.add_argument(
        "--model", "-m",
        choices=["gpt4o", "claude", "o3mini", "deepseek"],
        default="gpt4o",
        help="Model to use for processing",
    )
    process_parser.add_argument(
        "--prompt", "-p",
        type=str,
        default="Please process the following content:\n\n{content}",
        help="Prompt template to use",
    )
    process_parser.add_argument(
        "--system", "-s",
        type=str,
        default="You are a helpful AI assistant.",
        help="System message for the model",
    )
    
    # Parse arguments
    if args is None:
        args = sys.argv[1:]
    
    parsed_args = parser.parse_args(args)
    
    # Process command
    if parsed_args.command == "process":
        from pedster.processors.llm_processor import GPT4OProcessor, PromptTemplate
        
        # Set up ingestor to read from stdin
        ingestor = CLIIngestor()
        input_data = ingestor.ingest()
        
        if not input_data:
            print("No input provided. Pipe some text to the command.", file=sys.stderr)
            sys.exit(1)
        
        # Create prompt template
        prompt_template = PromptTemplate(
            template=parsed_args.prompt,
            system_message=parsed_args.system,
        )
        
        # Create processor
        processor = GPT4OProcessor(
            prompt_template=prompt_template,
        )
        
        # Process data
        result = processor.process(input_data[0])
        
        # Print result
        if result.success:
            print(result.data.content)
        else:
            print(f"Error: {result.error_message}", file=sys.stderr)
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
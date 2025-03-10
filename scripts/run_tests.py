#!/usr/bin/env python3
"""
Run all tests for Pedster with coverage reports.

Usage:
    python run_tests.py [--verbose] [--coverage] [--fail-fast] [--junit-xml JUNIT_XML]

This script runs all the Pedster tests using pytest and optionally generates coverage reports.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run Pedster tests")
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--coverage", "-c",
        action="store_true",
        help="Generate coverage report",
    )
    parser.add_argument(
        "--fail-fast", "-f",
        action="store_true",
        help="Stop on first failure",
    )
    parser.add_argument(
        "--junit-xml",
        type=str,
        help="Generate JUnit XML report",
    )
    parser.add_argument(
        "--pattern", "-p",
        type=str,
        help="Pattern for test files (e.g., 'test_cli_ingestor.py')",
    )
    return parser.parse_args()


def run_tests(args: argparse.Namespace) -> int:
    """Run tests with provided arguments."""
    # Build pytest command
    cmd = ["pytest"]
    
    # Add verbosity
    if args.verbose:
        cmd.append("-v")
    
    # Add fail-fast option
    if args.fail_fast:
        cmd.append("-x")
    
    # Add coverage options
    if args.coverage:
        cmd.extend([
            "--cov=pedster",
            "--cov-report=term",
            "--cov-report=html:coverage_html",
        ])
    
    # Add JUnit XML report option
    if args.junit_xml:
        cmd.append(f"--junitxml={args.junit_xml}")
    
    # Add test pattern if provided
    if args.pattern:
        cmd.append(f"tests/pedster/*/{args.pattern}")
    else:
        cmd.append("tests/")
    
    # Print command for debugging
    print(f"Running: {' '.join(cmd)}", file=sys.stderr)
    
    # Run pytest
    return subprocess.run(cmd).returncode


def check_environment() -> None:
    """Check if the environment is properly set up."""
    # Check if we're in the right directory
    if not os.path.exists("pedster") or not os.path.exists("tests"):
        print("Error: Run this script from the root of the Pedster project", file=sys.stderr)
        sys.exit(1)
    
    # Check if pytest is installed
    try:
        subprocess.run(["pytest", "--version"], 
                       stdout=subprocess.PIPE, 
                       stderr=subprocess.PIPE, 
                       check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: pytest is not installed. Install with: pip install pytest", file=sys.stderr)
        sys.exit(1)
    
    # If coverage is requested, check if pytest-cov is installed
    if "--coverage" in sys.argv or "-c" in sys.argv:
        try:
            subprocess.run(["pytest", "--cov=pedster"], 
                           stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE)
        except subprocess.CalledProcessError:
            print("Error: pytest-cov is not installed. Install with: pip install pytest-cov", file=sys.stderr)
            sys.exit(1)


def main() -> None:
    """Main function."""
    check_environment()
    args = parse_args()
    
    # Create a .coveragerc file if coverage is requested and file doesn't exist
    if args.coverage and not os.path.exists(".coveragerc"):
        with open(".coveragerc", "w") as f:
            f.write("""[run]
source = pedster
omit = 
    tests/*
    */__pycache__/*
    */\.*/*

[report]
exclude_lines =
    pragma: no cover
    def __repr__
    raise NotImplementedError
    if __name__ == .__main__.:
    pass
    raise ImportError
""")
    
    # Run tests
    exit_code = run_tests(args)
    
    # If coverage was generated, print location of HTML report
    if args.coverage and exit_code == 0:
        coverage_path = os.path.abspath("coverage_html/index.html")
        print(f"\nCoverage report generated at file://{coverage_path}")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
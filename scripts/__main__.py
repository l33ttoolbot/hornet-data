"""
CLI entry point for running scripts as a module.

Usage:
    python -m scripts.cli --help
    python -m scripts.run_pipeline --help
"""

from .cli import cli

if __name__ == "__main__":
    cli()
"""Entry point for the application.

This module serves as the main entry point when executing the package
as a command-line application. It initializes the CLI interface and
delegates execution to the command handler.
"""

from samara.cli import cli

if __name__ == "__main__":  # pragma: no cover
    cli()

"""E2E test executor for running jobs in isolation."""

import json
import logging
import subprocess
from pathlib import Path

from .redirector import PathRedirector

logger = logging.getLogger(__name__)


class JobTestExecutor:
    """Executes job configurations in isolated test environment."""

    def __init__(self, job_path: Path, tmp_path: Path) -> None:
        """Initialize executor with job configuration path.

        Args:
            job_path: Path to job.json file
            tmp_path: Temporary directory for test execution
        """
        self.job_path = job_path
        self.job_dir = job_path.parent
        self.tmp_path = tmp_path
        self.isolated_config_path = tmp_path / "isolated_job.json"

    def prepare_isolated_config(self) -> dict:
        """Create isolated job configuration with redirected paths.

        Returns:
            Modified job configuration with tmp paths
        """
        with open(self.job_path, encoding="utf-8") as f:
            config = json.load(f)

        # Create path redirector for this test
        redirector = PathRedirector(source_dir=self.job_dir, target_dir=self.tmp_path)

        # Redirect output paths to tmp (inputs stay as-is)
        isolated_config = redirector.redirect_config(config)

        return isolated_config

    def execute(self) -> dict:
        """Execute job using CLI in production-like manner.

        Returns:
            The isolated config with tmp paths
        """
        # Prepare isolated configuration
        isolated_config = self.prepare_isolated_config()

        # Write isolated config to tmp location
        with open(self.isolated_config_path, "w", encoding="utf-8") as f:
            json.dump(isolated_config, f, indent=2)

        # Execute via CLI exactly as in production (with coverage tracking)
        result = subprocess.run(
            [
                "coverage",
                "run",
                "--parallel-mode",
                "--branch",
                "--source=samara",
                "-m",
                "samara",
                "--otlp-traces-endpoint",
                "http://jaeger:4318/v1/traces",
                "--otlp-metrics-endpoint",
                "http://prometheus:9090/api/v1/otlp/v1/metrics",
                "run",
                "--workflow-filepath",
                str(self.isolated_config_path),
                "--alert-filepath",
                str(self.isolated_config_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            logger.error("Command failed with exit code %d", result.returncode)
            logger.error("STDOUT:\n%s", result.stdout)
            logger.error("STDERR:\n%s", result.stderr)

        assert result.returncode == 0

        logger.debug("Job executed successfully:\n%s", result.stdout)

        return isolated_config

"""Path redirection utilities for test isolation."""

from pathlib import Path


class PathRedirector:
    """Redirects paths in job configurations to isolated test directory."""

    def __init__(self, source_dir: Path, target_dir: Path) -> None:
        """Initialize path redirector.

        Args:
            source_dir: Original test directory containing job.json
            target_dir: Target temporary directory for isolation
        """
        self.source_dir = source_dir
        self.target_dir = target_dir

    def redirect_config(self, config: dict) -> dict:
        """Redirect output paths in configuration to tmp directory.

        Args:
            config: Original job configuration

        Returns:
            Modified configuration with redirected output paths
        """
        for job in config["workflow"]["jobs"]:
            # Leave input paths as-is (no need to copy read-only files)

            # Redirect load paths (outputs) to tmp
            for load in job["loads"]:
                load["location"] = self._redirect_output_path(load["location"])
                load["schema_export"] = self._redirect_output_path(load["schema_export"])

        return config

    def _redirect_output_path(self, path: str) -> str:
        """Redirect output path to tmp outputs directory."""
        return str(self.target_dir / "outputs" / Path(path).name)

"""
Code quality tests for SnowDucks.

This module contains tests that ensure the codebase meets quality standards
including formatting (black), linting (flake8), and type checking (mypy).
"""

import subprocess
import sys
from pathlib import Path
import pytest


class TestCodeQuality:
    """
    Code quality checks matching the CI pipeline:
      - Black formatting (src/cli/ and test/python/)
      - Flake8 linting (src/cli/ and test/python/)
      - Mypy type checking (src/cli/)
    """

    @pytest.fixture(scope="class")
    def project_root(self) -> Path:
        """Project root directory."""
        return Path(__file__).parent.parent.parent

    def test_black_formatting(self, project_root: Path) -> None:
        """All Python files must be formatted with black (CI config)."""
        result1 = subprocess.run(
            [sys.executable, "-m", "black", "--check", str(project_root / "src/cli")],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        result2 = subprocess.run(
            [
                sys.executable,
                "-m",
                "black",
                "--check",
                str(project_root / "test/python"),
            ],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        if result1.returncode != 0 or result2.returncode != 0:
            pytest.fail(
                f"black formatting failed.\n"
                f"src/cli output:\n{result1.stdout}\n{result1.stderr}\n"
                f"test/python output:\n{result2.stdout}\n{result2.stderr}"
            )

    def test_flake8_linting(self, project_root: Path) -> None:
        """All Python files must pass flake8 linting (CI config)."""
        result1 = subprocess.run(
            [
                sys.executable,
                "-m",
                "flake8",
                str(project_root / "src/cli"),
                "--max-line-length=101",
                "--extend-ignore=E203,W503",
            ],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        result2 = subprocess.run(
            [
                sys.executable,
                "-m",
                "flake8",
                str(project_root / "test/python"),
                "--max-line-length=101",
                "--extend-ignore=E203,W503",
            ],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        if result1.returncode != 0 or result2.returncode != 0:
            pytest.fail(
                f"flake8 linting failed.\n"
                f"src/cli output:\n{result1.stdout}\n{result1.stderr}\n"
                f"test/python output:\n{result2.stdout}\n{result2.stderr}"
            )

    def test_mypy_type_checking(self, project_root: Path) -> None:
        """All Python files must pass mypy type checking (CI config)."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "mypy",
                str(project_root / "src/cli"),
                "--ignore-missing-imports",
            ],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        if result.returncode != 0:
            pytest.fail(
                f"mypy type checking failed.\nOutput:\n{result.stdout}\n{result.stderr}"
            )

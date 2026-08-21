"""Unit tests for standalone binary packaging script."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from scripts.build_standalone_binary import (
    build_pyinstaller_command,
    get_target_platform_tag,
    main,
    resolve_target_names,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


class TestStandaloneBinaryPackaging:
    """Tests for standalone binary build configuration and command generation."""

    def test_get_target_platform_tag(self):
        """Platform tag reflects OS and normalized architecture."""
        tag = get_target_platform_tag()
        assert "-" in tag
        assert any(
            tag.startswith(os_name) for os_name in ("linux", "darwin", "windows")
        )

    def test_get_target_platform_tag_linux_x86_64(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Linux x86_64 maps to 'linux-x86_64'."""
        monkeypatch.setattr("platform.system", lambda: "Linux")
        monkeypatch.setattr("platform.machine", lambda: "x86_64")
        assert get_target_platform_tag() == "linux-x86_64"

    def test_get_target_platform_tag_darwin_arm64(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Darwin arm64 maps to 'darwin-arm64'."""
        monkeypatch.setattr("platform.system", lambda: "Darwin")
        monkeypatch.setattr("platform.machine", lambda: "arm64")
        assert get_target_platform_tag() == "darwin-arm64"

    def test_resolve_target_names_custom_and_default(self):
        """Target names resolve correctly with and without custom names."""
        name, base = resolve_target_names("linux-x86_64", "custom-bouncer")
        assert name == "custom-bouncer"
        assert base == "custom-bouncer"

        name, base = resolve_target_names("windows-x86_64")
        assert name == "dbt-bouncer-windows-x86_64.exe"
        assert base == "dbt-bouncer-windows-x86_64"

        name, base = resolve_target_names("darwin-arm64")
        assert name == "dbt-bouncer-darwin-arm64"
        assert base == "dbt-bouncer-darwin-arm64"

    def test_build_pyinstaller_command_default(self, tmp_path: Path):
        """PyInstaller command generates correct onefile and dependency flags."""
        cmd = build_pyinstaller_command(tmp_path)

        assert "-m" in cmd
        assert "PyInstaller" in cmd
        assert "--onefile" in cmd
        assert "--clean" in cmd
        assert "--distpath" in cmd
        assert str(tmp_path) in cmd
        assert "dbt_bouncer" in cmd
        assert "sqlglot" in cmd

    def test_build_pyinstaller_command_custom_name(self, tmp_path: Path):
        """Custom binary name is respected in command line flags."""
        custom_name = "custom-dbt-bouncer-bin"
        cmd = build_pyinstaller_command(tmp_path, binary_name=custom_name)

        name_idx = cmd.index("--name")
        assert cmd[name_idx + 1] == custom_name

    def test_main_success_flow(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """main() succeeds when PyInstaller returns returncode 0."""
        mock_run = MagicMock(
            return_value=subprocess.CompletedProcess(args=[], returncode=0)
        )
        monkeypatch.setattr("subprocess.run", mock_run)

        exit_code = main(["--output-dir", str(tmp_path)])
        assert exit_code == 0
        assert mock_run.called

    def test_main_failure_flow(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """main() returns failure code when PyInstaller fails."""
        mock_run = MagicMock(
            return_value=subprocess.CompletedProcess(args=[], returncode=1)
        )
        monkeypatch.setattr("subprocess.run", mock_run)

        exit_code = main(["--output-dir", str(tmp_path)])
        assert exit_code == 1

    def test_main_verify_flow(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """main() with --verify checks the built binary."""
        binary_file = tmp_path / "dbt-bouncer-linux-x86_64"
        binary_file.write_text("binary-stub")

        mock_run = MagicMock(
            side_effect=[
                subprocess.CompletedProcess(args=[], returncode=0),
                subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="dbt-bouncer 1.0.0"
                ),
                subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="Usage: dbt-bouncer"
                ),
            ]
        )
        monkeypatch.setattr("subprocess.run", mock_run)
        monkeypatch.setattr("platform.system", lambda: "Linux")
        monkeypatch.setattr("platform.machine", lambda: "x86_64")

        exit_code = main(["--output-dir", str(tmp_path), "--verify"])
        assert exit_code == 0
        # build (PyInstaller) + --version + --help smoke tests
        assert mock_run.call_count == 3

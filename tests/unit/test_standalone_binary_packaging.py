"""Unit tests for standalone binary packaging script."""

from __future__ import annotations

from typing import TYPE_CHECKING

from scripts.build_standalone_binary import (
    build_pyinstaller_command,
    get_target_platform_tag,
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

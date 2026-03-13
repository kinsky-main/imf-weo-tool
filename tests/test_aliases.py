from __future__ import annotations

from pathlib import Path

from weo_tools.legacy import load_alias_config, normalize_label


def test_load_alias_config_supports_multi_code_subject_aliases(tmp_path: Path) -> None:
    alias_path = tmp_path / "aliases.toml"
    alias_path.write_text(
        "\n".join(
            [
                '[subjects]',
                '"Gross domestic product, current prices" = ["NGDP", "NGDPD"]',
                '[countries]',
                '"United Kingdom" = "GBR"',
            ]
        ),
        encoding="utf-8",
    )

    aliases = load_alias_config(alias_path)

    assert aliases.subjects["gross domestic product current prices"] == ["NGDP", "NGDPD"]
    assert aliases.countries["united kingdom"] == ["GBR"]
    assert normalize_label("U.S. dollars") == "u s dollars"

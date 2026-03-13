from __future__ import annotations

from types import SimpleNamespace

import pytest

import weo_to_dataframe


class _Parser:
    def parse_args(self):
        return SimpleNamespace(config=None)


def test_weo_to_dataframe_prints_concise_error_for_invalid_interactive_query(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(weo_to_dataframe, "build_common_parser", lambda description: _Parser())
    monkeypatch.setattr(weo_to_dataframe, "load_defaults", lambda config: SimpleNamespace())
    monkeypatch.setattr(weo_to_dataframe, "merge_settings", lambda defaults, args: SimpleNamespace(output_path=None))
    monkeypatch.setattr(weo_to_dataframe, "ImfWeoClient", lambda: object())
    monkeypatch.setattr(
        weo_to_dataframe,
        "run_dataframe",
        lambda settings, client: (_ for _ in ()).throw(ValueError("Availability lookup failed for NGDPD: Invalid availability selection")),
    )
    monkeypatch.setattr(weo_to_dataframe.sys, "argv", ["weo_to_dataframe.py"])

    with pytest.raises(SystemExit) as exc_info:
        weo_to_dataframe.main()

    assert exc_info.value.code == 1
    assert capsys.readouterr().err.strip() == "Availability lookup failed for NGDPD: Invalid availability selection"

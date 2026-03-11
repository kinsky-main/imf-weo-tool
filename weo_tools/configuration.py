from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
import argparse
import tomllib


DEFAULT_CONFIG_PATH = Path("config/weo_defaults.toml")


@dataclass(slots=True)
class RuntimeSettings:
    countries: list[str] = field(default_factory=list)
    subject_descriptors: list[str] = field(default_factory=list)
    units: list[str] = field(default_factory=list)
    scales: list[str] = field(default_factory=list)
    frequency: str = "A"
    start_year: int | None = None
    end_year: int | None = None
    interactive: bool = False
    output_path: str = ""
    compatibility_workbook: str = "data/weoapr2025all.xlsx"
    alias_file: str = "config/weo_aliases.toml"


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)]


def load_defaults(path: str | Path = DEFAULT_CONFIG_PATH) -> RuntimeSettings:
    config_path = Path(path)
    if not config_path.exists():
        return RuntimeSettings()

    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)

    return RuntimeSettings(
        countries=_as_list(raw.get("countries")),
        subject_descriptors=_as_list(raw.get("subject_descriptors")),
        units=_as_list(raw.get("units")),
        scales=_as_list(raw.get("scales")),
        frequency=str(raw.get("frequency", "A")),
        start_year=raw.get("start_year"),
        end_year=raw.get("end_year"),
        interactive=bool(raw.get("interactive", False)),
        output_path=str(raw.get("output_path", "")),
        compatibility_workbook=str(raw.get("compatibility_workbook", "data/weoapr2025all.xlsx")),
        alias_file=str(raw.get("alias_file", "config/weo_aliases.toml")),
    )


def merge_settings(defaults: RuntimeSettings, args: argparse.Namespace) -> RuntimeSettings:
    merged = replace(defaults)

    if args.country:
        merged.countries = list(args.country)
    if args.subject_descriptor:
        merged.subject_descriptors = list(args.subject_descriptor)
    if args.unit:
        merged.units = list(args.unit)
    if args.scale:
        merged.scales = list(args.scale)
    if args.frequency:
        merged.frequency = args.frequency
    if args.start_year is not None:
        merged.start_year = args.start_year
    if args.end_year is not None:
        merged.end_year = args.end_year
    if args.output:
        merged.output_path = args.output
    if args.compatibility_workbook:
        merged.compatibility_workbook = args.compatibility_workbook
    if args.alias_file:
        merged.alias_file = args.alias_file
    if args.interactive:
        merged.interactive = True

    return merged


def build_common_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to the TOML defaults file.")
    parser.add_argument("--country", action="append", help="Legacy country selector. Repeat for multiple countries.")
    parser.add_argument(
        "--subject-descriptor",
        action="append",
        help="Legacy subject descriptor selector. Repeat for multiple series.",
    )
    parser.add_argument("--unit", action="append", help="Legacy units selector. Repeat for multiple values.")
    parser.add_argument("--scale", action="append", help="Legacy scale selector. Repeat for multiple values.")
    parser.add_argument("--frequency", default=None, help="Frequency code. Defaults to A.")
    parser.add_argument("--start-year", type=int, default=None, help="First year to include.")
    parser.add_argument("--end-year", type=int, default=None, help="Last year to include.")
    parser.add_argument("--output", default=None, help="Optional output file path.")
    parser.add_argument("--interactive", action="store_true", help="Prompt for missing selectors in a terminal UI.")
    parser.add_argument("--compatibility-workbook", default=None, help="Legacy WEO workbook used for old labels.")
    parser.add_argument("--alias-file", default=None, help="TOML file with extra label aliases.")
    return parser

from __future__ import annotations

import sys

from weo_tools.app import run_excel_export
from weo_tools.configuration import build_common_parser, load_defaults, merge_settings
from weo_tools.imf import ImfWeoClient


def main() -> None:
    parser = build_common_parser("Export IMF WEO time series to a legacy-style Excel workbook.")
    args = parser.parse_args()
    defaults = load_defaults(args.config)
    settings = merge_settings(defaults, args)
    if len(sys.argv) == 1:
        settings.interactive = True

    client = ImfWeoClient()
    output_path = run_excel_export(settings, client)
    release = client.fetch_catalog().release

    print(f"Saved Excel workbook to {output_path}")
    print(f"WEO release: {release.version} (updated {release.updated_at})")


if __name__ == "__main__":
    main()

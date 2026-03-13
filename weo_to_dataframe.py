from __future__ import annotations

import sys

from weo_tools.app import run_dataframe, save_dataframe
from weo_tools.configuration import build_common_parser, load_defaults, merge_settings
from weo_tools.imf import ImfWeoClient


def main() -> None:
    parser = build_common_parser("Pull IMF WEO time series into a pandas dataframe.")
    args = parser.parse_args()
    defaults = load_defaults(args.config)
    settings = merge_settings(defaults, args)
    if len(sys.argv) == 1:
        settings.interactive = True

    try:
        client = ImfWeoClient()
        dataframe = run_dataframe(settings, client)
        release = client.fetch_catalog().release
    except KeyboardInterrupt:
        print("Selection cancelled.", file=sys.stderr)
        raise SystemExit(130) from None
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from None

    if settings.output_path:
        output_path = save_dataframe(dataframe, settings.output_path)
        print(f"Saved dataframe to {output_path}")

    print(f"WEO release: {release.version} (updated {release.updated_at})")
    print(f"Rows: {len(dataframe)}")
    if not dataframe.empty:
        print(dataframe.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

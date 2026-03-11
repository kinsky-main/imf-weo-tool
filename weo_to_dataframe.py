from __future__ import annotations

from weo_tools.app import run_dataframe, save_dataframe
from weo_tools.configuration import build_common_parser, load_defaults, merge_settings
from weo_tools.imf import ImfWeoClient


def main() -> None:
    parser = build_common_parser("Pull IMF WEO time series into a pandas dataframe.")
    args = parser.parse_args()
    defaults = load_defaults(args.config)
    settings = merge_settings(defaults, args)

    client = ImfWeoClient()
    dataframe = run_dataframe(settings, client)
    release = client.fetch_catalog().release

    if settings.output_path:
        output_path = save_dataframe(dataframe, settings.output_path)
        print(f"Saved dataframe to {output_path}")

    print(f"WEO release: {release.version} (updated {release.updated_at})")
    print(f"Rows: {len(dataframe)}")
    if not dataframe.empty:
        print(dataframe.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

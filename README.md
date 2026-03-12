# IMF WEO Pull Scripts

This repo contains two small Python entrypoints for the IMF World Economic
Outlook database:

- `weo_dataframe_demo.ipynb` is the primary analysis example and shows how to
  pull WEO data with `pysdmx`.
- `weo_to_dataframe.py` remains as a thin dataframe/export wrapper for
  automation.
- `weo_to_excel.py` exports the selected series to a legacy-style wide Excel
  sheet that starts with `Country`, `Subject Descriptor`, `Units`, and `Scale`.

The backend uses `pysdmx[all]` for SDMX query construction and IMF service
access, targeting the latest stable WEO release via `IMF.RES/WEO/+`.

## Setup

Windows PowerShell:

```powershell
./scripts/setup_env.ps1
.\.venv\Scripts\Activate.ps1
```

Unix shell:

```sh
./scripts/setup_env.sh
. .venv/bin/activate
```

## Configuration

Defaults live in `config/weo_defaults.toml`. CLI flags override the file. If
you set `interactive = true` or pass `--interactive`, the script will open a
terminal selector for missing inputs.

The scripts accept the legacy selector fields:

- `Country`
- `Subject Descriptor`
- `Units`
- `Scale`

Legacy label compatibility is sourced from `data/weoapr2025all.xlsx` and the
editable overrides in `config/weo_aliases.toml`.

## Examples

Pull a dataframe and write a CSV:

```powershell
python weo_to_dataframe.py --country "United Kingdom" --subject-descriptor "Gross domestic product, current prices" --unit "U.S. dollars" --scale Billions --start-year 2020 --end-year 2030 --output output/gbr_ngdpd.csv
```

Export the same selection to Excel:

```powershell
python weo_to_excel.py --country "United Kingdom" --subject-descriptor "Gross domestic product, current prices" --unit "U.S. dollars" --scale Billions --output output/gbr_ngdpd.xlsx
```

Interactive selection:

```powershell
python weo_to_excel.py
```

If you launch either script with no arguments, it opens a searchable multi-select
TUI for each required selector. Type to filter the list, use `Up`/`Down` to
move, `Space` to mark items, and press `Enter` to confirm each step. The app
shows a loading spinner while it checks valid next-step selectors and while it
fetches the data.

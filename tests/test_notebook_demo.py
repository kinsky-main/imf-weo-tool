from __future__ import annotations

import json
from pathlib import Path


def test_notebook_demo_uses_valid_weo_query_pattern() -> None:
    notebook = json.loads(Path("weo_dataframe_demo.ipynb").read_text(encoding="utf-8"))
    sources = "\n".join("".join(cell.get("source", [])) for cell in notebook["cells"])

    assert "ApiVersion.V2_2_2" in sources
    assert "read_sdmx" in sources
    assert 'key=f"{country_code}.{indicator_code}.{frequency}"' in sources
    assert "weo_frame['UNIT']" in sources
    assert "weo_frame['SCALE']" in sources
    assert "TextFilter" not in sources
    assert "c[SCALE]" not in sources

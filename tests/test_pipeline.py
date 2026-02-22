from pathlib import Path

from scripts import run_water_bodies_pipeline


def test_resolve_resource_url_passthrough() -> None:
    direct = "https://example.com/water.csv"
    assert run_water_bodies_pipeline.resolve_resource_url("https://catalog.data.gov/dataset/water-bodies-07739", direct) == direct


def test_ensure_dirs_creates_expected_tree(tmp_path: Path) -> None:
    dirs = run_water_bodies_pipeline.ensure_dirs(tmp_path)
    assert dirs["raw"].exists()
    assert dirs["processed"].exists()
    assert dirs["models"].exists()
    assert dirs["figures"].exists()
    assert dirs["spark_ui"].exists()

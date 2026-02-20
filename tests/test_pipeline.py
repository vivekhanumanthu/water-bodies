from pathlib import Path

from scripts import run_pipeline


def test_first_existing_returns_first_match() -> None:
    cols = ["x", "y", "z"]
    assert run_pipeline.first_existing(cols, ["a", "y", "z"]) == "y"


def test_ensure_dirs_creates_expected_tree(tmp_path: Path) -> None:
    dirs = run_pipeline.ensure_dirs(tmp_path)
    assert dirs["json_chunks"].exists()
    assert dirs["models"].exists()
    assert dirs["processed"].exists()
    assert dirs["figures"].exists()

from lib.config import load_config

def test_load_config_from_file(tmp_path):
    p = tmp_path / "cfg.json"
    p.write_text('{"dev": {"k":"v"}}')
    out = load_config(f"file://{p}")
    assert out["dev"]["k"] == "v"

import json
from typing import Dict

def load_config(uri: str, *, spark=None) -> Dict:
    """
    Load a JSON config from file:// or s3a://.
    If spark is provided, s3a is supported without additional code.
    """
    if uri.startswith("file://"):
        path = uri.replace("file://", "")
        with open(path, "r") as f:
            return json.load(f)
    if uri.startswith("s3a://"):
        if spark is None:
            raise ValueError("Spark session required for s3a:// config reads.")
        df = spark.read.text(uri)
        raw = "\n".join(r.value for r in df.collect())
        return json.loads(raw)
    raise ValueError(f"Unsupported CONFIG_S3_URI: {uri}")

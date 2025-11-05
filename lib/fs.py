import glob
import os
from typing import Optional

def must_exist_glob(uri: str, *, label: str, if_scheme: Optional[str] = "file"):
    """
    For local 'file://' URIs, ensure at least one file matches the glob.
    Skips checks for non-'file' schemes (e.g., s3a) so jobs still run in AWS.
    """
    if if_scheme and not uri.startswith(f"{if_scheme}://"):
        return
    pattern = uri.replace(f"{if_scheme}://", "")
    matches = glob.glob(pattern)
    if not matches:
        raise FileNotFoundError(f"{label} missing: no files match {pattern}")

def ensure_dir(uri: str, *, scheme="file"):
    """
    Ensure a local directory exists (useful for file:// targets).
    """
    if not uri.startswith(f"{scheme}://"):
        return
    path = uri.replace(f"{scheme}://", "")
    os.makedirs(path, exist_ok=True)

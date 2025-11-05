import sys
from awsglue.utils import getResolvedOptions

REQUIRED = ["ENV", "CONFIG_S3_URI", "BOOKMARKED"]

def parse_args(argv=None):
    """
    Parse Glue-style arguments. Returns a dict.
    """
    argv = argv or sys.argv
    return getResolvedOptions(argv, REQUIRED)

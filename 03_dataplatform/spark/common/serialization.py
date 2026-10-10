"""JSON normalization helpers shared by the API and pipeline."""
import math
from datetime import date, datetime
from decimal import Decimal


def normalize(value):
    if hasattr(value, "asDict"):
        return normalize(value.asDict(recursive=True))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    return str(value)

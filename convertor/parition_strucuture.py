from typing import List, Dict, Optional
from convertor.utils import HIVE_TO_DATABRICKS_MAP


def generate_partition_definitions(
    partitions: List[Dict], skewed_columns: Optional[List[str]] = None
) -> str:
    seen = set()

    partition_defs = [
        f"{p['name']} {HIVE_TO_DATABRICKS_MAP.get(p['type'].lower(), p['type'].upper())}"
        for p in partitions
        if p.get("name") and p.get("type") and not seen.add(p["name"])
    ]

    if skewed_columns:
        for col in skewed_columns:
            if col not in seen:
                partition_defs.append(f"{col} STRING")  # Default to STRING

    return ",\n  ".join(partition_defs)

from typing import List, Dict, Optional
from convertor.datatype_mapping import HIVE_TO_DATABRICKS_MAP


def generate_partition_definitions(
    partitions: List[Dict], skewed_columns: Optional[List[str]] = None
) -> str:
    seen = set()
    partition_defs = []

    for part in partitions:
        name = part.get("name")
        hive_type = part.get("type")

        if not name or not hive_type or name in seen:
            continue

        mapped_type = HIVE_TO_DATABRICKS_MAP.get(hive_type.lower(), hive_type.upper())
        partition_defs.append(f"{name} {mapped_type}")
        seen.add(name)

    for col in skewed_columns or []:
        if col not in seen:
            partition_defs.append(f"{col} STRING")
            seen.add(col)

    return ",\n  ".join(partition_defs)

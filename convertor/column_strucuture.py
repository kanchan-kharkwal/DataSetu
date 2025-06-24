from typing import List, Dict
from convertor.utils import HIVE_TO_DATABRICKS_MAP


def generate_column_definitions(columns: List[Dict]) -> str:
    column_defs = []
    for col in columns:
        name = col.get("name")
        hive_type = col.get("type")
        if not name or not hive_type:
            continue
        mapped_type = HIVE_TO_DATABRICKS_MAP.get(hive_type, hive_type)

        column_defs.append(f"{name} {mapped_type}")
    return ",\n  ".join(column_defs)

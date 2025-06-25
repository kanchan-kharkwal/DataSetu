from typing import List, Dict
from convertor.datatype_mapping import TypeMapper


def generate_column_definitions(columns: List[Dict]) -> str:
    column_defs = []

    for col in columns:
        name = col.get("name")
        hive_type = col.get("type")
        comment = col.get("comment", "")

        if not name or not hive_type:
            continue

        mapped_type = TypeMapper.map_type(hive_type)
        comment_clause = f" COMMENT '{comment}'" if comment else ""

        column_defs.append(f"{name} {mapped_type}{comment_clause}")

    return ",\n  ".join(column_defs)

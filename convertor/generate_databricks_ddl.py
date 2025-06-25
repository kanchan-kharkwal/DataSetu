from convertor.column_structure import generate_column_definitions
from convertor.partition_structure import generate_partition_definitions
from typing import List, Dict


def generate_create_table_ddl(
    table_name: str,
    columns: List[Dict],
    partitions: List[Dict],
    table_type: str,
    location: str = "",
    skewed_cols: List[str] = [],
    file_format: str = "DELTA"
) -> str:
    column_defs = generate_column_definitions(columns)
    partition_defs = generate_partition_definitions(partitions, skewed_cols)

    ddl = f"CREATE {'EXTERNAL ' if table_type == 'EXTERNAL_TABLE' else ''}TABLE IF NOT EXISTS {table_name} (\n  {column_defs}\n)"

    if partition_defs:
        ddl += f"\nPARTITIONED BY (\n  {partition_defs}\n)"
        
    ddl += f"\nUSING {file_format.upper()}"

    if table_type == "EXTERNAL_TABLE" and location:
        ddl += f"\nLOCATION '{location}'"

    return ddl

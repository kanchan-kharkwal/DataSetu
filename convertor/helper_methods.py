from typing import Optional
import os

def generate_location_clause(table_type: str, location: str) -> str:
    if table_type == "EXTERNAL_TABLE" and location:
        return f"\nLOCATION '{location}'"
    return ""


def has_column_defaults(columns: list[dict]) -> bool:
    return any("default" in col and col["default"] for col in columns)


def generate_properties_clause(
    properties: dict, constraints: dict, columns: list[dict]
) -> str:
    try:
        props = {k.strip(): v for k, v in properties.items() if k and k.strip()}

        if has_column_defaults(columns):
            props["delta.feature.allowColumnDefaults"] = "enabled"

        for name, expr in constraints.get("check_constraints", {}).items():
            props[f"delta.constraints.{name}"] = expr

        if not props:
            return ""

        props_str = ",\n  ".join(f"'{k}' = '{v}'" for k, v in props.items())
        return f"\nTBLPROPERTIES (\n  {props_str}\n)"
    except Exception as e:
        print(f"[ERROR] Failed to generate table properties: {e}")
        return ""


def generate_optimize_statement(
    bucket_cols: list, table_name: str, database_name: str
) -> Optional[str]:
    try:
        if not bucket_cols:
            return None

        zorder_cols = ", ".join(
            col.strip() for col in bucket_cols if col and col.strip()
        )
        if not zorder_cols:
            return None

        return (
            f"-- OPTIMIZE {database_name}.{table_name} ZORDER BY ({zorder_cols}); "
            f"COMMENT 'ZORDER statement for bucketing optimization TO BE APPLIED AT RUNTIME'"
        )
    except Exception as e:
        print(f"[ERROR] Failed to generate OPTIMIZE statement: {e}")
        return None

def infer_format(input_format) -> str:

    format_mapping = {
        "orc": "DELTA",
        "parquet": "PARQUET",
        "textinputformat": "CSV",
        "avro": "AVRO"
    }

    return next(
        (fmt for key, fmt in format_mapping.items() if key in input_format),
        "DELTA"
    )

def export_ddl_to_sql(output_dir: str, db: str, table: str, ddl: str):
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{db}.{table}.sql")
    with open(file_path, "w") as f:
        f.write(ddl)

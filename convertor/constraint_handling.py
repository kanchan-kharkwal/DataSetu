from typing import Dict, List, Any

def _format_columns(columns: List[str]) -> str:
    """Format list of columns into SQL string."""
    return ", ".join(columns)


def _generate_constraint_sql(constraint_type: str, table: str, constraint: Dict[str, Any]) -> str:
    """Generate ALTER TABLE statement for primary, unique, and foreign key constraints."""
    name = constraint["constraint_name"]
    cols = _format_columns(constraint["columns"])

    if constraint_type == "primary_key":
        return f"ALTER TABLE {table} ADD CONSTRAINT {name} PRIMARY KEY ({cols})"

    elif constraint_type == "unique":
        return f"ALTER TABLE {table} ADD CONSTRAINT {name} UNIQUE ({cols})"

    elif constraint_type == "foreign_key":
        ref_table = constraint["reference_table"]
        ref_cols = _format_columns(constraint["reference_columns"])
        return (
            f"ALTER TABLE {table} ADD CONSTRAINT {name} "
            f"FOREIGN KEY ({cols}) REFERENCES {ref_table} ({ref_cols})"
        )

    return ""


def generate_alter_statements(
    table: str,
    constraints: Dict[str, Any],
    types: List[str] = ["primary_key", "unique", "foreign_key"]
) -> List[str]:
    """Generate ALTER TABLE statements for supported constraint types."""
    statements = []

    for ctype in types:
        constraint_data = constraints.get(ctype)
        if not constraint_data:
            continue

        if isinstance(constraint_data, dict):  # For primary_key case
            constraint_data = [constraint_data]

        for constraint in constraint_data:
            sql = _generate_constraint_sql(ctype, table, constraint)
            if sql:
                statements.append(sql)

    return statements


def extract_column_constraints(constraints: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Extract NOT NULL and DEFAULT column constraints for DDL modification."""
    col_constraints: Dict[str, Dict[str, Any]] = {}

    for nn in constraints.get("not_null", []):
        col_constraints.setdefault(nn["column"], {})["not_null"] = True

    for df in constraints.get("default", []):
        col_constraints.setdefault(df["column"], {})["default"] = df["default_value"]

    return col_constraints


def generate_tblproperties(constraints: Dict[str, Any]) -> Dict[str, str]:
    """Generate TBLPROPERTIES for Databricks including enabling default column values and check constraints."""
    props = {
        "delta.feature.defaultColumnValues": "supported"  # Required for enabling default values
    }

    for chk in constraints.get("check", []):
        props[f"check.{chk['constraint_name']}"] = chk["check_condition"]

    return props


def generate_all_constraints(table: str, constraints_json: Dict[str, Any]) -> Dict[str, Any]:
    """Master function to generate all Databricks constraints: ALTER, column mods, TBLPROPERTIES."""
    constraints = constraints_json.get("constraints", {})

    return {
        "alter_statements": generate_alter_statements(table, constraints),
        "column_modifications": extract_column_constraints(constraints),
        "table_properties": generate_tblproperties(constraints)
    }

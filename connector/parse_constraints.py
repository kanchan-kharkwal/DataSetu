def extract_foreign_key_data(col0: str, col1: str) -> tuple[str, str, int]:
    """
    Extracts parent column, child column, and key sequence from foreign key metadata.
    """
    try:
        parent_column = None
        column = None
        key_sequence = None

        # Split the string by multiple spaces or tabs
        parts = col0.strip().split("  ")
        for part in parts:
            if "Parent Column Name" in part:
                parent_column = part.split(":", 1)[1].strip()
            elif "Column Name" in part:
                column = part.split(":", 1)[1].strip()
            elif "Key Sequence" in part:
                key_sequence = int(part.split(":", 1)[1].strip())

        return parent_column, column, key_sequence

    except Exception as e:
        print(f"[ERROR] Failed to extract foreign key data from '{col0}': {e}")
        return "", "", -1


def parse_constraints(constraints_section: list[tuple]) -> dict:
    result = {
        "not_null": [],
        "default": [],
        "check": [],
        "primary_key": None,
        "foreign_keys": [],
    }

    current_table = None

    for row in constraints_section:
        if "Table:" in row[0]:
            current_table = row[1]
            continue

        if "Constraint Name:" in row[0]:
            constraint_name = row[1]
            continue

        if "Column Name:" in row[0]:
            column_name = row[1]
            continue

        if "Default Value:" in row[0]:
            result["default"].append(
                {
                    "table": current_table,
                    "column": column_name,
                    "constraint": constraint_name,
                    "default_value": row[1],
                }
            )

        elif "Check Value:" in row[0]:
            result["check"].append(
                {
                    "table": current_table,
                    "constraint": constraint_name,
                    "expression": row[1],
                }
            )

        elif "Key Sequence:" in row[0] and "Parent Column Name" in row[0]:
            # Extract from full row[0] and row[1]
            parent_col, column, key_seq = extract_foreign_key_data(row[0], row[1])
            result["foreign_keys"].append(
                {
                    "table": current_table,
                    "constraint": constraint_name,
                    "column": column,
                    "parent_column": parent_col,
                    "key_sequence": key_seq,
                }
            )

        elif "Column Names:" in row[0]:
            result["primary_key"] = {
                "table": current_table,
                "constraint": constraint_name,
                "columns": [c.strip() for c in row[1].split(",")],
            }

        elif "NOT NULL" in row[0] or "Not Null" in row[0]:
            result["not_null"].append(
                {
                    "table": current_table,
                    "column": column_name,
                    "constraint": constraint_name,
                }
            )

    return result

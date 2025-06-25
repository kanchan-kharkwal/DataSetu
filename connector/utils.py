import ast
from connector.parse_constraints import parse_constraints

def execute(connection, sql_text: str):
    if not connection:
        raise RuntimeError("No active Hive connection")

    cursor = connection.cursor()
    try:
        cursor.execute(sql_text)
        return cursor.fetchall()
    finally:
        cursor.close()


def convert_sections_to_clean_json(db: str, table: str, sections: dict) -> dict:
    def clean_kv(rows):
        return {
            k.strip().rstrip(":"): v.strip()
            for row in rows
            if len(row) >= 2 and (k := row[0]) and (v := row[1])
        }

    def extract_columns(rows):
        return [
            {
                "name": name.strip(),
                "type": dtype.strip(),
                "comment": comment.strip() if comment else "",
            }
            for name, dtype, comment in rows
            if name.lower() != "# col_name"
        ]

    def parse_list_string(value):
        try:
            return ast.literal_eval(value) if value else []
        except Exception:
            return [val.strip() for val in value.strip("[]").split(",") if val.strip()]

    def safe_extract(rows, start_key):
        result, capture = {}, False
        for row in rows:
            if len(row) < 2:
                continue
            k, v = row[0], row[1]
            if k.strip() == start_key:
                capture = True
                continue
            if capture and k.strip():
                result[k.strip()] = v.strip()
        return result

    # Extract constraints
    constraints_raw = sections.get("constraints", []) + sections.get(
        "not_null_constraints", []
    )
    constraint_data = parse_constraints(constraints_raw)

    # Extract other parts
    table_info = clean_kv(sections.get("table_info", []))
    storage_info = clean_kv(sections.get("storage_info", []))

    bucket_cols = parse_list_string(storage_info.get("Bucket Columns", ""))
    sort_cols = parse_list_string(storage_info.get("Sort Columns", ""))
    skewed_cols = parse_list_string(storage_info.get("Skewed Columns", ""))
    skewed_vals = parse_list_string(storage_info.get("Skewed Values", ""))

    desc_params = safe_extract(sections.get("storage_info", []), "Storage Desc Params:")
    table_params = safe_extract(sections.get("table_info", []), "Table Parameters:")

    return {
        "database": db,
        "table_name": table,
        "location": table_info.get("Location", ""),
        "table_type": table_info.get("Table Type", ""),
        "columns": extract_columns(sections.get("columns", [])),
        "partitions": extract_columns(sections.get("partitions", [])),
        "storage_format": {
            "input_format": storage_info.get("InputFormat", ""),
            "output_format": storage_info.get("OutputFormat", ""),
            "serde_library": storage_info.get("SerDe Library", ""),
            "compressed": storage_info.get("Compressed", ""),
            "bucket_columns": bucket_cols,
            "sort_columns": sort_cols,
            "num_buckets": int(storage_info.get("Num Buckets", "0")),
            "stored_as_subdirectories": storage_info.get(
                "Stored As SubDirectories", ""
            ).lower()
            == "yes",
            "skewed_columns": skewed_cols,
            "skewed_values": skewed_vals,
            "desc_params": desc_params,
        },
        "table_parameters": table_params,
        "meta": {
            "owner": table_info.get("Owner", ""),
            "created_at": table_info.get("CreateTime", ""),
            "last_accessed": table_info.get("LastAccessTime", ""),
            "retention": table_info.get("Retention", ""),
        },
        "constraints": constraint_data,
    }

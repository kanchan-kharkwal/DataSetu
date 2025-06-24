def execute(connection, sql_text: str):
    if not connection:
        raise RuntimeError("No active Hive connection")

    cursor = connection.cursor()
    try:
        cursor.execute(sql_text)
        return cursor.fetchall()
    finally:
        cursor.close()


import ast


def convert_sections_to_clean_json(db: str, table: str, sections: dict) -> dict:
    def clean_kv(rows):
        return {k.strip().rstrip(":"): v.strip() for k, v in rows if k.strip()}

    def extract_columns(rows):
        return [
            {"name": name.strip(), "type": dtype.strip()}
            for name, dtype in rows
            if name.lower() != "# col_name"
        ]

    # Parse and clean base sections
    table_info = clean_kv(sections.get("table_info", []))
    storage_info = clean_kv(sections.get("storage_info", []))

    # Safely parse complex fields using ast.literal_eval
    def parse_list_string(value):
        try:
            return ast.literal_eval(value) if value else []
        except Exception:
            return [val.strip() for val in value.strip("[]").split(",") if val.strip()]

    bucket_cols = parse_list_string(storage_info.get("Bucket Columns", ""))
    sort_cols = parse_list_string(storage_info.get("Sort Columns", ""))
    skewed_cols = parse_list_string(storage_info.get("Skewed Columns", ""))
    skewed_vals = parse_list_string(storage_info.get("Skewed Values", ""))

    # Extract Storage Desc Params manually from rows
    desc_params = {}
    rows = sections.get("storage_info", [])
    capturing = False
    for k, v in rows:
        if k.strip() == "Storage Desc Params:":
            capturing = True
            continue
        if capturing and k.strip():
            desc_params[k.strip()] = v.strip()

    # Extract table parameters section (after "Table Parameters:")
    table_params = {}
    param_section_found = False
    for k, v in sections.get("table_info", []):
        if k.strip() == "Table Parameters:":
            param_section_found = True
            continue
        if param_section_found and k.strip():
            table_params[k.strip()] = v.strip()

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
    }

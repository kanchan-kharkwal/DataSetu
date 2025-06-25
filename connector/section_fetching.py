def split_describe_formatted(description_rows: list[tuple]) -> dict:
    section_mapping = {
        "# Partition Information": "partitions",
        "# Detailed Table Information": "table_info",
        "# Storage Information": "storage_info",
        "# SerDe Library": "serde_info",
        "# Not Null Constraints": "not_null_constraints",
        "# Constraints": "constraints",
    }

    try:
        sections = {section: [] for section in section_mapping.values()}
        sections.update({"columns": [], "others": []})

        current_section = "columns"

        for row in description_rows:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue

            col0 = row[0].strip() if len(row) > 0 and row[0] else ""
            col1 = row[1].strip() if len(row) > 1 and row[1] else ""
            col2 = row[2].strip() if len(row) > 2 and row[2] else ""

            header = col0

            if header in section_mapping:
                current_section = section_mapping[header]
                continue

            if not any([col0, col1]):
                continue

            sections[current_section].append((col0, col1, col2))

        return sections

    except Exception as e:
        print(f"[ERROR] Failed to split DESCRIBE FORMATTED output: {e}")
        return {}

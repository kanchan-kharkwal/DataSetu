def split_describe_formatted(description_rows: list[tuple]) -> dict:
    section_mapping = {
        "# Partition Information": "partitions",
        "# Detailed Table Information": "table_info",
        "# Storage Information": "storage_info",
        "# SerDe Library": "serde_info",
        "# Not Null Constraints": "not_null_constraints",
        "# Constraints": "constraints",
    }

    sections = {section: [] for section in section_mapping.values()}
    sections.update({"columns": [], "others": []})

    current_section = "columns"

    for row in description_rows:
        col0 = row[0].strip() if row[0] else ""
        col1 = row[1].strip() if row[1] else ""

        header = col0

        if header in section_mapping:
            current_section = section_mapping[header]
            continue

        if not any([col0, col1]):
            continue

        sections[current_section].append((col0, col1))

    return sections

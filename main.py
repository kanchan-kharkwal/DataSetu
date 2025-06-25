import yaml
import os
import json
from connector.connection import ConnectionToHive
from connector.db_function import list_databases, list_tables, describe_formatted
from connector.section_fetching import split_describe_formatted
from connector.utils import convert_sections_to_clean_json
from convertor.generate_databricks_ddl import generate_create_table_ddl
from convertor.helper_methods import (
    generate_properties_clause,
    generate_optimize_statement,
    infer_format,
)


def load_hive_config(path: str = "config/creds.yaml") -> dict:
    full_path = os.path.join(os.path.dirname(__file__), path)
    with open(full_path, "r") as f:
        return yaml.safe_load(f)


def export_clean_json(output_dir: str, db: str, table: str, clean_json: dict) -> None:
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{db}.{table}_clean.json")
    with open(file_path, "w") as f:
        json.dump(clean_json, f, indent=2)


def process_table(hive_conn, db: str, table: str) -> None:
    print(f"Running DESCRIBE FORMATTED for: {db}.{table}")
    description = describe_formatted(hive_conn._conn, db, table)
    sections = split_describe_formatted(description)
    clean_json = convert_sections_to_clean_json(db, table, sections)
    export_clean_json("metadata_output", db, table, clean_json)

    columns = clean_json.get("columns", [])
    partitions = clean_json.get("partitions", [])
    storage_format = clean_json.get("storage_format", {})
    skewed_cols = storage_format.get("skewed_columns", [])
    table_type = clean_json.get("table_type", "")
    location = clean_json.get("location", "")
    constraints = clean_json.get("constraints", {})
    properties = clean_json.get("table_parameters", {})

    input_format = storage_format.get("input_format", "")
    file_format = infer_format(input_format)

    full_table_name = f"{db}.{table}"

    ddl = generate_create_table_ddl(
        table_name=full_table_name,
        columns=columns,
        partitions=partitions,
        table_type=table_type,
        location=location,
        skewed_cols=skewed_cols,
        file_format=file_format,
    )

    ddl += generate_properties_clause(properties, constraints, columns)

    bucket_cols = storage_format.get("bucket_columns", [])
    optimize_stmt = generate_optimize_statement(bucket_cols, table, db)
    if optimize_stmt:
        ddl += f"\n{optimize_stmt}"

    print(ddl)


def main():
    config = load_hive_config()
    hive_conn = ConnectionToHive(config)
    hive_conn.connect()

    option = (
        input(
            "Type 'user' to enter databases manually or 'all' to fetch all from Hive: "
        )
        .strip()
        .lower()
    )
    if option == "user":
        user_input = input("Enter list of databases (comma-separated): ").strip()
        databases = [db.strip() for db in user_input.split(",") if db.strip()]
    elif option == "all":
        databases = list_databases(hive_conn._conn)
    else:
        print("Invalid option. Please type 'user' or 'all'.")
        hive_conn.close()
        return

    for db in databases:
        print(f"\nTables in database '{db}':")
        try:
            tables = list_tables(hive_conn._conn, db)
            for table in tables:
                try:
                    process_table(hive_conn, db, table)
                except Exception as e:
                    print(f"Failed to describe table: {e}")
        except Exception as e:
            print(f"Error fetching tables for '{db}': {e}")

    hive_conn.close()


if __name__ == "__main__":
    main()

import yaml
import os
import json
from connector.connection import ConnectionToHive
from connector.db_function import list_databases, list_tables, describe_formatted
from connector.section_fetching import split_describe_formatted
from connector.utils import convert_sections_to_clean_json
from convertor.column_strucuture import generate_column_definitions
from convertor.parition_strucuture import generate_partition_definitions


def load_hive_config(path: str = "config/creds.yaml") -> dict:
    full_path = os.path.join(os.path.dirname(__file__), path)
    with open(full_path, "r") as f:
        return yaml.safe_load(f)


def main():
    config = load_hive_config()
    hive_conn = ConnectionToHive(config)
    hive_conn.connect()

    choose_database_fetch = (
        input(
            "Type 'user' to enter databases manually or 'all' to fetch all from Hive: "
        )
        .strip()
        .lower()
    )

    match choose_database_fetch:
        case "user":
            user_input = input("Enter list of databases (comma-separated): ").strip()
            databases = [db.strip() for db in user_input.split(",") if db.strip()]
        case "all":
            databases = list_databases(hive_conn._conn)
        case _:
            print("Invalid option. Please type 'user' or 'all'.")
            hive_conn.close()
            return

    for db in databases:
        print(f"\nTables in database '{db}':")
        try:
            tables = list_tables(hive_conn._conn, db)
            if tables:
                for table in tables:
                    try:
                        print(f"Running DESCRIBE FORMATTED for: {db}.{table}")
                        description = describe_formatted(hive_conn._conn, db, table)
                        sections = split_describe_formatted(description)
                        clean_json = convert_sections_to_clean_json(db, table, sections)

                        output_dir = "metadata_output"
                        os.makedirs(output_dir, exist_ok=True)
                        json_filename = f"{output_dir}/{db}.{table}_clean.json"
                        with open(json_filename, "w") as f:
                            json.dump(clean_json, f, indent=2)

                        columns = clean_json.get("columns") or []
                        partitions = clean_json.get("partitions") or []
                        skewed_cols = (
                            clean_json.get("storage_format", {}).get("skewed_columns")
                            or []
                        )

                        column_defs = generate_column_definitions(columns)
                        partition_defs = generate_partition_definitions(
                            partitions, skewed_cols
                        )

                    except Exception as e:
                        print(f"Failed to describe table: {e}")

            else:
                print("(No tables found)")
        except Exception as e:
            print(f"Error fetching tables for '{db}': {e}")

    hive_conn.close()


if __name__ == "__main__":
    main()

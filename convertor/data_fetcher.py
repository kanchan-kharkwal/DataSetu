class HiveMetadataFetcher():
    def __init__(self, hive_json: dict):
        if not isinstance(hive_json, dict):
            print("Warning: Input is not a dictionary")
        self.schema = hive_json

    def extract_metadata(self) -> dict:
        try:
            return {
                "database": self.schema.get("database"),
                "table_name": self.schema.get("table_name"),
                "columns": self.schema.get("columns", []),
                "partitions": self.schema.get("partitions", []),
                "storage_format": self.schema.get("storage_format", {}),
                "location": self.schema.get("location", ""),
                "table_type": self.schema.get("table_type", "MANAGED_TABLE"),
                "table_parameters": self.schema.get("table_parameters", {})
            }
        except Exception as e:
            print(f"Error extracting metadata: {e}")
            return {}

class HiveToDatabricksDDLConverter:

    def __init__(self, hive_json: dict, catalog_name: str = "main"):

        self.schema = hive_json

        self.catalog = catalog_name
 
        # Identifiers

        self.db_name = self.schema.get("database")

        self.table_name = self.schema.get("table_name")

        self.full_table_name = f"{self.catalog}.{self.db_name}.{self.table_name}"
 
        # Columns and partition info

        self.columns = self.schema.get("columns", [])

        self.partitions = self.schema.get("partitions", [])
 
        # Storage & metadata

        storage = self.schema.get("storage_format", {})

        self.bucket_cols = storage.get("bucket_columns", [])

        self.num_buckets = storage.get("num_buckets", 0)

        self.location = self._clean_location(self.schema.get("location", ""))

        self.table_type = self.schema.get("table_type", "MANAGED_TABLE")

        self.properties = self.schema.get("table_parameters", {})
 
    def _clean_location(self, loc: str) -> str:

        if loc.startswith("file:/"):

            return loc.replace("file:/", "dbfs:/")

        return loc
 
    def _map_type(self, hive_type: str) -> str:

        hive_type = hive_type.lower().strip()
 
        if hive_type.startswith("array<"):

            inner = hive_type[6:-1]

            return f"ARRAY<{self._map_type(inner)}>"

        elif hive_type.startswith("map<"):

            key, val = hive_type[4:-1].split(",", 1)

            return f"MAP<{self._map_type(key)}, {self._map_type(val)}>"

        elif hive_type.startswith("struct<"):

            fields = hive_type[7:-1]

            mapped = []

            for field in fields.split(","):

                k, v = field.split(":")

                mapped.append(f"{k.strip()}:{self._map_type(v.strip())}")

            return f"STRUCT<{', '.join(mapped)}>"

        elif hive_type.startswith("decimal"):

            return hive_type.upper()

        elif hive_type.startswith("varchar") or hive_type.startswith("char"):

            return "STRING"
 
        mapping = {

            "int": "INT",

            "tinyint": "TINYINT",

            "smallint": "SMALLINT",

            "bigint": "BIGINT",

            "float": "FLOAT",

            "double": "DOUBLE",

            "boolean": "BOOLEAN",

            "string": "STRING",

            "timestamp": "TIMESTAMP",

            "date": "DATE",

            "binary": "BINARY"

        }

        return mapping.get(hive_type, hive_type.upper())
 
    def _generate_column_definitions(self) -> str:

        lines = []

        for col in self.columns:

            name = col["name"]

            dtype = self._map_type(col["type"])

            lines.append(f"  {name} {dtype}")

        return ",\n".join(lines)
 
    def _generate_partition_clause(self) -> str:

        if not self.partitions:

            return ""

        parts = ", ".join([

            f"{p['name']} {self._map_type(p['type'])}" for p in self.partitions

        ])

        return f"\nPARTITIONED BY ({parts})"
 
    def _generate_location_clause(self) -> str:

        if self.table_type == "EXTERNAL_TABLE" and self.location:

            return f"\nLOCATION '{self.location}'"

        return ""
 
    def _generate_properties_clause(self) -> str:

        props = {k: v for k, v in self.properties.items() if k.strip()}

        if not props:

            return ""

        props_str = ",\n  ".join([f"'{k}' = '{v}'" for k, v in props.items()])

        return f"\nTBLPROPERTIES (\n  {props_str}\n)"
 
    def _infer_format(self) -> str:

        input_format = self.schema.get("storage_format", {}).get("input_format", "").lower()

        if "orc" in input_format:

            return "DELTA"  # Switch ORC to DELTA for Databricks best practice

        elif "parquet" in input_format:

            return "PARQUET"

        elif "textinputformat" in input_format:

            return "CSV"

        elif "avro" in input_format:

            return "AVRO"

        return "DELTA"
 
    def generate_ddl(self) -> str:

        ddl = f"CREATE {'EXTERNAL' if self.table_type == 'EXTERNAL_TABLE' else ''} TABLE {self.full_table_name} (\n"

        ddl += self._generate_column_definitions()

        ddl += "\n)"

        ddl += f"\nUSING {self._infer_format()}"

        ddl += self._generate_partition_clause()

        ddl += self._generate_location_clause()

        ddl += self._generate_properties_clause()

        return ddl
 
    def generate_optimize_statement(self) -> str | None:

        """Return ZORDER statement if table used bucketing originally"""

        if self.bucket_cols:

            zorder_cols = ", ".join(self.bucket_cols)

            return f"OPTIMIZE {self.full_table_name} ZORDER BY ({zorder_cols});"

        return None

 
if __name__ == "__main__":

    import json
 
    with open(r"C:\Users\BAPS\Desktop\DS-New\connector\1.json") as f:

        hive_json = json.load(f)
 
    converter = HiveToDatabricksDDLConverter(hive_json)

    ddl = converter.generate_ddl()

    zorder = converter.generate_optimize_statement()
 
    print("=== DDL ===")

    print(ddl)

    if zorder:

        print("\n=== ZORDER ===")

        print(zorder)

 
 
    
# import json
 
# with open(r"C:\Users\BAPS\Desktop\DS-New\connector\1.json") as f:
#     hive_data = json.load(f)
 
# converter = HiveToDatabricksDDLConverter(hive_data, catalog_name="bhumi_catalog")
# ddl_script = converter.generate_ddl()
# print(ddl_script)
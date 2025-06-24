
from type_mapping import TypeMapper

class DatabricksDDLGenerator:
    def __init__(self, metadata: dict, catalog: str = "main"):
        self.catalog = catalog
        self.db_name = metadata["database"]
        self.table_name = metadata["table_name"]
        self.full_table_name = f"{self.catalog}.{self.db_name}.{self.table_name}"
        
        self.columns = metadata["columns"]
        self.partitions = metadata["partitions",[]]
        self.storage_format = metadata["storage_format"]
        self.bucket_cols = self.storage_format.get("bucket_columns", [])
        self.num_buckets = self.storage_format.get("num_buckets", 0)
        self.skewed_cols = self.storage_format.get("skewed_columns", [])
        
        self.location = metadata.get("location", "")
        self.table_type = metadata.get("table_type", "MANAGED_TABLE")
        self.properties = metadata.get("table_parameters", {})
        self.constraints = metadata.get("constraints", {}) if "constraints" in metadata else {}
        

    def _generate_column_definitions(self) -> str:
        lines = []
        pk_cols = set(self.constraints.get("primary_key", []))
        for col in self.columns:
            name = col["name"]
            dtype = TypeMapper.map_type(col["type"])
            constraint = " NOT NULL" if name in pk_cols else ""
            lines.append(f"  {name} {dtype}{constraint}")
        return ",\n".join(lines)
    

    def _generate_partition_clause(self) -> str:
        if not self.partitions:
            return ""
        
        # Adding Hive partitions
        partition_defs = [
            (p['name'], TypeMapper.map_type(p['type'])) for p in self.partitions
        ]

        # Add skewed columns if not already present
        existing = {p[0] for p in partition_defs}
        for col in self.skewed_cols:
            if col not in existing:
                dtype = self._get_column_type(col)
                if dtype:
                    partition_defs.append((col, TypeMapper.map_type(dtype)))

        if not partition_defs:
            return ""
        part_str = ", ".join([f"{name} {dtype}" for name, dtype in partition_defs])
        return f"\nPARTITIONED BY ({part_str})"


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
    
    def _generate_primary_key_comment(self) -> str:
        if pk := self.constraints.get("primary_key"):
            return f"\n-- Primary Key: {', '.join(pk)}"
        return ""


    def _infer_format(self) -> str:
        input_format = self.storage_format.get("input_format", "").lower()
        if "orc" in input_format:
            return "DELTA"
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
        if self.bucket_cols:
            zorder_cols = ", ".join(self.bucket_cols)
            return f"OPTIMIZE {self.full_table_name} ZORDER BY ({zorder_cols});"
        return None

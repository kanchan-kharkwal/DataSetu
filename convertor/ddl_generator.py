
from type_mapping import TypeMapper

class DatabricksDDLGenerator:
    def __init__(self, metadata: dict, catalog: str = "main"):
        self.catalog = catalog
        self.db_name = metadata["database"]
        self.table_name = metadata["table_name"]
        self.full_table_name = f"{self.catalog}.{self.db_name}.{self.table_name}"
        
        self.columns = metadata["columns"]
        self.partitions = metadata.get("partitions",[])
        self.storage_format = metadata["storage_format"]
        self.bucket_cols = self.storage_format.get("bucket_columns", [])
        self.num_buckets = self.storage_format.get("num_buckets", 0)
        self.skewed_cols = self.storage_format.get("skewed_columns", [])
        
        self.location = metadata.get("location", "")
        self.table_type = metadata.get("table_type", "MANAGED_TABLE")
        self.properties = metadata.get("table_parameters", {})
        self.constraints = metadata.get("constraints", {}) if "constraints" in metadata else {}
        
    def _has_column_defaults(self) -> bool:
        return any("default" in col for col in self.columns)
        

    def _generate_column_definitions(self) -> str:
        try:
            not_null_cols = set(self.constraints.get("not_null", []))
            lines = []
            
            for col in self.columns:
                name = col["name"]
                hive_type = col.get("type")

                if not name or not hive_type:
                    print(f"❗ Warning: Column '{name}' has missing or empty type. Skipping.")
                    continue  
                    
                dtype = TypeMapper.map_type(col["type"])
                
                default_val = col.get("default")
                column_def = f"  {name} {dtype}"
                
                if default_val is not None:
                    if isinstance(default_val, str):
                        default_val = f"'{default_val}'"
                    column_def += f" DEFAULT {default_val}"
                
                if name in not_null_cols:
                    column_def += " NOT NULL"
                lines.append(column_def)
            return ",\n".join(lines)
        
        except Exception as e:
            print(f"Error generating column definitions: {e}")
            return ""
    

    def generate_partition_clause(self) -> str:
        try:
            if not self.partitions:
                return ""
            
            # Adding Hive partitions
            partition_cols = [
                p['name'] for p in self.partitions
            ]

            # Adding skewed columns if not present
            partition_cols = [p['name'] for p in self.partitions]

            for col in self.skewed_cols:
                if col not in partition_cols:
                    partition_cols.append(col)

            if not partition_cols:
                return ""

            part_str = ", ".join(partition_cols)
            return f"\nPARTITIONED BY ({part_str})"
        except Exception as e:
            print(f"Error generating partition clause: {e}")
            return ""


    def generate_location_clause(self) -> str:
        try:
            if self.table_type == "EXTERNAL_TABLE" and self.location:
                return f"\nLOCATION '{self.location}'"
        except Exception as e:
            print(f"[ERROR] Failed to generate location clause: {e}")
        return ""

    def generate_properties_clause(self) -> str:
        try:
            props = {k: v for k, v in self.properties.items() if k.strip()}
            
            if self._has_column_defaults():
                props["delta.feature.allowColumnDefaults"] = "enabled"
            
            for name, expr in self.constraints.get("check_constraints", {}).items():
                props[f"delta.constraints.{name}"] = expr
            
            if not props:
                return ""
            
            props_str = ",\n  ".join([f"'{k}' = '{v}'" for k, v in props.items()])
            return f"\nTBLPROPERTIES (\n  {props_str}\n)"
        except Exception as e:
            print(f"[ERROR] Failed to generate table properties: {e}")
            return ""
    
    def _generate_primary_key_comment(self) -> str:
        try:
            if pk := self.constraints.get("primary_key"):
                return f"\n-- Primary Key: {', '.join(pk)}"
        except Exception as e:
            print(f"[ERROR] Failed to generate PK comment: {e}")
        return ""


    def _infer_format(self) -> str:
        try:
            input_format = self.storage_format.get("input_format", "").lower()
            if "orc" in input_format:
                return "DELTA"
            elif "parquet" in input_format:
                return "PARQUET"
            elif "textinputformat" in input_format:
                return "CSV"
            elif "avro" in input_format:
                return "AVRO"
        except Exception as e:
            print(f"[ERROR] Failed to infer file format: {e}")
        return "DELTA"

    def generate_ddl(self) -> str:
        try:
            if self.table_type == 'EXTERNAL_TABLE' and not self.location:
                return f"-- ❗ ERROR: EXTERNAL TABLE '{self.full_table_name}' missing LOCATION clause"

            ddl = f"CREATE {'EXTERNAL' if self.table_type == 'EXTERNAL_TABLE' else ''} TABLE {self.full_table_name} (\n"
            ddl += self._generate_column_definitions()
            ddl += "\n)"
            ddl += f"\nUSING {self._infer_format()}"
            ddl += self.generate_partition_clause()
            ddl += self.generate_location_clause()
            ddl += self.generate_properties_clause()
            return ddl
        except Exception as e:
            print(f"[ERROR] Failed to generate full DDL for {self.full_table_name}: {e}")
            return f"-- Failed to generate DDL for {self.full_table_name}"

    def generate_optimize_statement(self) -> str | None:
        try:
            if self.bucket_cols:
                zorder_cols = ", ".join(self.bucket_cols)
                return f"-- OPTIMIZE {self.full_table_name} ZORDER BY ({zorder_cols}); COMMENT 'ZORDER statement for bucketing optimization TO BE APPLIED AT RUNTIME'"
        except Exception as e:
            print(f"[ERROR] Failed to generate OPTIMIZE statement: {e}")
        return None

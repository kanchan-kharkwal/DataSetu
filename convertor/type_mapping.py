class TypeMapper:
    @staticmethod
    def map_type(hive_type: str) -> str:
        try:
            hive_type = hive_type.lower().strip()

            if hive_type.startswith("array<"):
                inner = hive_type[6:-1]
                return f"ARRAY<{TypeMapper.map_type(inner)}>"

            elif hive_type.startswith("map<"):
                key, val = hive_type[4:-1].split(",", 1)
                return f"MAP<{TypeMapper.map_type(key)}, {TypeMapper.map_type(val)}>"

            elif hive_type.startswith("struct<"):
                fields = hive_type[7:-1]
                mapped = []
                for field in fields.split(","):
                    k, v = field.split(":")
                    mapped.append(f"{k.strip()}:{TypeMapper.map_type(v.strip())}")
                return f"STRUCT<{', '.join(mapped)}>"
            
            elif hive_type.startswith("uniontype<"):
                union_types = hive_type[10:-1].split(",")
                fields = []
                for i, ut in enumerate(union_types):
                    dtype = TypeMapper.map_type(ut.strip())
                    fields.append(f"field_{i}:{dtype}")
                return f"STRUCT<{', '.join(fields)}>"

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

        except Exception as e:
            print(f"[ERROR] Failed to map Hive type '{hive_type}': {e}")
            return "STRING"  # Safe fallback

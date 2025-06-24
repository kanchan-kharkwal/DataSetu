class TypeMapper:

    @staticmethod
    def map_type(hive_type: str) -> str:
        try:
            hive_type = hive_type.lower().strip()

            # Handle parameterized types like decimal(10,2), varchar(100), etc.
            base_type = hive_type.split("(", 1)[0]

            # Dispatch for complex types
            for complex_prefix, handler in TypeMapper.COMPLEX_TYPE_HANDLERS.items():
                if hive_type.startswith(complex_prefix):
                    return handler(hive_type)

            if base_type == "decimal":
                return hive_type.upper()

            # Simple type mapping
            return TypeMapper.SIMPLE_TYPE_MAP.get(base_type, hive_type.upper())

        except Exception as e:
            print(f"[ERROR] Failed to map Hive type '{hive_type}': {e}")
            return "STRING"

    @staticmethod
    def _handle_array(hive_type: str) -> str:
        inner = hive_type[6:-1]
        return f"ARRAY<{TypeMapper.map_type(inner)}>"

    @staticmethod
    def _handle_map(hive_type: str) -> str:
        key_val = hive_type[4:-1].split(",", 1)
        if len(key_val) == 2:
            key, val = key_val
            return f"MAP<{TypeMapper.map_type(key)}, {TypeMapper.map_type(val)}>"
        return "MAP<STRING, STRING>"

    @staticmethod
    def _handle_struct(hive_type: str) -> str:
        fields = hive_type[7:-1].split(",")
        mapped_fields = []
        for field in fields:
            if ":" in field:
                k, v = field.split(":")
                mapped_fields.append(f"{k.strip()}:{TypeMapper.map_type(v.strip())}")
        return f"STRUCT<{', '.join(mapped_fields)}>"

    @staticmethod
    def _handle_union(hive_type: str) -> str:
        union_types = hive_type[10:-1].split(",")
        fields = [
            f"field_{i}:{TypeMapper.map_type(t.strip())}"
            for i, t in enumerate(union_types)
        ]
        return f"STRUCT<{', '.join(fields)}>"

    # Dispatch table for complex types
    COMPLEX_TYPE_HANDLERS = {
        "array<": _handle_array.__func__,
        "map<": _handle_map.__func__,
        "struct<": _handle_struct.__func__,
        "uniontype<": _handle_union.__func__,
    }

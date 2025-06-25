HIVE_TO_DATABRICKS_MAP = {
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
    "binary": "BINARY",
    "varchar": "STRING",
    "char": "STRING",
}


class TypeMapper:
    COMPLEX_TYPE_HANDLERS = {}

    @staticmethod
    def map_type(hive_type: str) -> str:
        try:
            hive_type = hive_type.strip().lower()
            for prefix, handler in TypeMapper.COMPLEX_TYPE_HANDLERS.items():
                if hive_type.startswith(prefix) and hive_type.endswith(">"):
                    return handler(hive_type)

            if hive_type.startswith("decimal"):
                return hive_type.upper()

            base_type = hive_type.split("(", 1)[0]
            return HIVE_TO_DATABRICKS_MAP.get(base_type, hive_type.upper())

        except Exception as e:
            print(f"[ERROR] Failed to map Hive type '{hive_type}': {e}")
            return "STRING"

    @staticmethod
    def _safe_split(s: str, delimiter: str = ",") -> list:
        parts = []
        current = ""
        depth = 0
        for c in s:
            if c == "<":
                depth += 1
            elif c == ">":
                depth -= 1
            if c == delimiter and depth == 0:
                parts.append(current)
                current = ""
            else:
                current += c
        if current:
            parts.append(current)
        return parts

    @staticmethod
    def _handle_array(hive_type: str) -> str:
        inner = hive_type[len("array<") : -1].strip()
        return f"ARRAY<{TypeMapper.map_type(inner)}>"

    @staticmethod
    def _handle_map(hive_type: str) -> str:
        inner = hive_type[len("map<") : -1].strip()
        parts = TypeMapper._safe_split(inner, ",")
        if len(parts) == 2:
            key, val = parts
            return f"MAP<{TypeMapper.map_type(key.strip())}, {TypeMapper.map_type(val.strip())}>"
        return "MAP<STRING, STRING>"

    @staticmethod
    def _handle_struct(hive_type: str) -> str:
        inner = hive_type[len("struct<") : -1].strip()
        fields = TypeMapper._safe_split(inner)
        struct_parts = []
        for field in fields:
            if ":" in field:
                name, type_ = field.split(":", 1)
                struct_parts.append(
                    f"{name.strip()}:{TypeMapper.map_type(type_.strip())}"
                )
        return f"STRUCT<{', '.join(struct_parts)}>"

    @staticmethod
    def _handle_union(hive_type: str) -> str:
        inner = hive_type[len("uniontype<") : -1].strip()
        types = TypeMapper._safe_split(inner)
        fields = [
            f"field_{i}:{TypeMapper.map_type(t.strip())}" for i, t in enumerate(types)
        ]
        return f"STRUCT<{', '.join(fields)}>"


TypeMapper.COMPLEX_TYPE_HANDLERS = {
    "array<": TypeMapper._handle_array,
    "map<": TypeMapper._handle_map,
    "struct<": TypeMapper._handle_struct,
    "uniontype<": TypeMapper._handle_union,
}

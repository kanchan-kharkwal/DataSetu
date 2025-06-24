from connector.utils import execute


def list_databases(connection) -> list[str]:
    return [r[0] for r in execute(connection, "SHOW DATABASES")]


def list_tables(connection, database) -> list[str]:
    return [r[0] for r in execute(connection, f"SHOW TABLES IN {database}")]


def describe_formatted(connection, database: str, table: str) -> list[tuple]:
    query = f"DESCRIBE FORMATTED {database}.{table}"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()

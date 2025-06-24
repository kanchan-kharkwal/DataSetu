from connector.utils import execute

def list_databases(connection) -> list[str]:
    try:
        return [r[0] for r in execute(connection, "SHOW DATABASES")]
    except Exception as e:
        print(f"[ERROR] Failed to list databases: {e}")
        return []

def list_tables(connection, database) -> list[str]:
    try:
        return [r[0] for r in execute(connection, f"SHOW TABLES IN {database}")]
    except Exception as e:
        print(f"[ERROR] Failed to list tables in database '{database}': {e}")
        return []

def describe_formatted(connection, database: str, table: str) -> list[tuple]:
    query = f"DESCRIBE FORMATTED {database}.{table}"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"[ERROR] Failed to describe table '{database}.{table}': {e}")
        return []
    finally:
        cursor.close()

from pyhive import hive

class ConnectionToHive:

    def __init__(self, config: dict):
        self.user = config["user"]
        self.password = config["password"]
        self.host = config["host"]
        self.port = config["port"]
        self.database = config["database"]
        self.auth = config["auth"]
        self._conn = None

    def connect(self) -> None:
        try:
            self._conn = hive.Connection(
                host=self.host,
                port=self.port,
                username=self.user,
                database=self.database,
                auth=self.auth,
            )
            print(f"[INFO] Connected to Hive at {self.host}:{self.port}, DB: {self.database}")
        except Exception as e:
            print(f"[ERROR] Failed to connect to Hive: {e}")
            self._conn = None

    def close(self) -> None:
        try:
            if self._conn:
                self._conn.close()
                print("[INFO] Hive connection closed")
                self._conn = None
        except Exception as e:
            print(f"[ERROR] Failed to close Hive connection: {e}")

from pyhive import hive


class ConnectionToHive:

    def __init__(self, config: dict):
        self.user = config["user"]
        self.password = config["password"]
        self.host = config["host"]
        self.port = config["port"]
        self.database = config["database"]
        self.auth = config["auth"]

    def connect(self) -> None:
        self._conn = hive.Connection(
            host=self.host,
            port=self.port,
            username=self.user,
            database=self.database,
            auth=self.auth,
        )
        print(f"Connected to Hive at {self.host}:{self.port}, DB: {self.database}")

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            print("Connection Closed")
            self._conn = None

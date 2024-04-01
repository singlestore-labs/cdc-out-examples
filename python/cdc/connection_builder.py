from typing import Optional
from MySQLdb import Connection, connect


class ConnectionBuilder:
    def __init__(self, host: str, port: int, user: str | None = None, db: str | None = None, passwd: str | None = None):
        self.host = host
        self.port = port
        self.user = user or "root"
        self.passwd = passwd
        self.db = db

    def __call__(self, db: Optional[str] = None) -> Connection:
        """Create a connection to the under-test MemSQL instance"""

        if self.passwd is None:
            return connect(
                host=self.host,  # type: ignore[attr-defined]
                port=self.port,  # type: ignore[attr-defined]
                user=self.user,
                database=db or self.db or "",
            )
        return connect(
            host=self.host,  # type: ignore[attr-defined]
            port=self.port,  # type: ignore[attr-defined]
            user=self.user,
            passwd=self.passwd,
            database=db or self.db or "",
        )

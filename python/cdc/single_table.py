from cdc.table_definition import TableDefinition
from MySQLdb import Connection


class SinglestoreTable:
    """
    Context manager for a table that is created and dropped on enter/exit.
    The table will contain a single column with the provided definition.
    """

    def __init__(self, conn: Connection, definition: TableDefinition):
        self._conn = conn
        self._definition = definition

    def __enter__(self):
        self._conn.query(self._definition.drop_sql())
        self._conn.query(self._definition.create_sql())
        return self._definition.name, self._conn

    def __exit__(self, *args):
        self._conn.query(self._definition.drop_sql())
        pass

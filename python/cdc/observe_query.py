import threading

from typing import Callable, Any, List, Optional, Union, Sequence
from binascii import hexlify
from MySQLdb import cursors, Connection, MySQLError

from cdc.connection_builder import ConnectionBuilder
from cdc.record import CdcRecord
from cdc.table_definition import TableDefinition

OBSERVE_AUX_COLUMN_NAMES = ['Offset', 'PartitionId', 'Type', 'Table', 'TxId', 'TxPartitions', 'InternalId']

VALID_OFFSET_BINARY_SIZE = 24


def zero_byte_string(size: int) -> bytes:
    return b'\0' * size


def single_offset(num_partitions: int, partition_id: int, offset: str | bytes) -> [str]:
    return [offset if i == partition_id else None for i in range(num_partitions)]


def single_default_offset(num_partitions: int, partition_id: int, make_offset: Callable[[int], Any]) -> [str]:
    return [None if i == partition_id else make_offset(i) for i in range(num_partitions)]


def serialize_offset(offset) -> str:
    """Return a string representing the offset for a given number"""
    if offset is None:
        return 'NULL'
    elif isinstance(offset, bytes):
        return f"'{hexlify(offset).decode('utf-8')}'"
    else:
        return f"'{offset}'"


class ObserveQuery:
    """
    Session class for a running observation query.
    Does the internal cursor & connection handling once finished.
    """

    def __init__(self, conn_builder: ConnectionBuilder,
                 tables: list[TableDefinition],
                 field_filters: Optional[List[str]] = None,
                 cdc_format: Optional[str] = None,
                 offsets: Sequence[Union[str, bytes, None]] = None,
                 db_name: Optional[str] = None,
                 timeout: Union[int, float] = 60):
        self._field_filters = field_filters or ['*']
        self._tables = tables
        self._cdc_format = cdc_format
        self._offsets = offsets
        self._conn_builder = conn_builder
        self._db_name = db_name or 'x_db'
        self._conn = None
        self._cursor = None
        self._timeout: float = float(timeout)
        self.timeout_func = None
        self._kill_conn = None

    @property
    def cursor(self) -> cursors.Cursor:
        assert self._cursor is not None, "Cursor is not initialized"
        return self._cursor

    def start(self, offsets: Sequence[Union[str, bytes, None]] | None = None):
        self._conn = self._conn_builder(self._db_name)
        self._kill_conn = self._conn_builder(self._db_name)
        query = f"OBSERVE {','.join(self._field_filters)} FROM {','.join((table.name for table in self._tables))}"
        if self._cdc_format:
            query += f" AS {self._cdc_format}"
        offsets = offsets or self._offsets
        if offsets is not None:
            query += f" BEGIN AT ({','.join(map(serialize_offset, offsets))})"
        print("Query:", query)
        # Run a timeout task
        if self.timeout_func is not None:
            self.timeout_func.cancel()
        self.timeout_func = threading.Timer(self._timeout, self._try_kill, args=[])
        self.timeout_func.start()
        # Use a cursor that keeps the result-set server-side until explicitly fetched
        self._cursor = self._conn.cursor(cursors.SSCursor)
        self._cursor.execute(query)

    def stop(self) -> None:
        if self.timeout_func is not None:
            self.timeout_func.cancel()
            self.timeout_func = None
        self._try_kill()
        try:
            self._cursor.close()
        except MySQLError as _e:
            pass

    def __enter__(self) -> cursors.Cursor:
        self.start()
        return self._cursor

    def __exit__(self, *args):
        self.stop()

    @staticmethod
    def _wait_for_kill(conn: Connection, pid: int):
        while True:
            cursor = conn.cursor()
            row_count = cursor.execute(f"SELECT * FROM information_schema.PROCESSLIST WHERE ID = {pid}")
            if row_count == 0:
                return
            res = cursor.fetchall()
            if not res:
                return

    def _try_kill(self):
        kill_conn = self._kill_conn
        self._kill_conn = None
        if kill_conn is not None:
            pid = self._conn.thread_id()
            kill_conn.kill(pid)
            self._wait_for_kill(kill_conn, pid)

    def restart_from(self, offsets: Sequence[Union[str, bytes, None]]):
        self.stop()
        self.start(offsets)


def wait_for_first_transaction(cursor: cursors.Cursor) -> CdcRecord:
    in_snapshot = False
    while True:
        record = CdcRecord(cursor.fetchone())
        if record.record_type == "BeginSnapshot":
            in_snapshot = True
        if record.record_type == "CommitSnapshot":
            in_snapshot = False
        if record.record_type == "BeginTransaction" and not in_snapshot:
            return record

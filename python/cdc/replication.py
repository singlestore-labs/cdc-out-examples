"""
Provides utilities for replicating data from SingleStoreDB to other MySQL-compatible databases,
using OBSERVE queries.
"""
import MySQLdb

from typing import Callable, List, Union, Optional
from threading import Event
from cdc.connection_builder import ConnectionBuilder
from cdc.gather import TransactionIterator
from cdc.observe_query import ObserveQuery
from cdc.table_definition import TableDefinition
from time import time

ER_DISTRIBUTED_DATABASE_NOT_SHARDED: int = 1795

ALLOWED_KILL_ERROR_CODES = {
    2013,  # Lost connection to server during a query
}


class TableReplication:
    """
    Replicates data from a single table in the source SingleStoreDB (cluster)
    to a MySQL-compatible database using an OBSERVE query.
    The class creates a table in the destination database matching the
    schema of the source table (with the addition of internal ID column),
     and then continuously replicates data from the source table.
    """

    def __init__(
        self,
        src_db: str,
        src_conn_builder: ConnectionBuilder,
        target_db: str,
        target_conn_builder: ConnectionBuilder,
        table: TableDefinition,
        target_table_name: Optional[str] = None,
        pause_every_n_seconds: Union[int, float] = 10,
        on_pause: Callable[[int], None] = lambda _: None,
        verbose: bool = False,
    ):
        # Makes connections to the source DB
        self.src_db = src_db
        self.src_conn_builder = src_conn_builder
        self.src_table = table  # The schema of the table to replicate
        self.target_db = target_db

        # The schema of the table to replicate to
        self.target_table = TableDefinition(target_table_name or f"{table.name}_replicated")
        # Add internal ID column to the target table
        self.target_table.column(name="internal_id", sql_type="int", nullable=False)
        # Copy all columns from the source table to the target table
        for column in table.columns:
            self.target_table.copy_column(column)

        # Makes connections to the target DB
        self.target_conn_builder = target_conn_builder

        # Figure out what the src DB's partition count is
        with self.src_conn_builder(None) as conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SHOW PARTITIONS")
                partitions = cursor.fetchall()
                self.partition_count = len(partitions) if partitions is not None else 1
            except MySQLdb.Error as e:
                if e.args[0] == ER_DISTRIBUTED_DATABASE_NOT_SHARDED:
                    self.partition_count = 1  # Singlebox DB
                else:
                    raise e

        self.offsets: List[Optional[str | bytes]] = [None] * self.partition_count  # The offsets to start observing from
        self._pause_every_n_seconds: float = float(pause_every_n_seconds)
        self._callback_between_observations: Callable = on_pause
        self._records_per_iterations: int = 0
        self._stop_token = Event()

        # Set up the table in the src & target DBs
        self._drop_and_create_tables()
        self._verbose = verbose

    def stop(self):
        """
        Stops the replication process.
        """
        self._stop_token.set()

    def _drop_and_create_tables(self):
        """
        Creates the table in the source & target databases.
        """
        with self.src_conn_builder() as conn:
            conn.query(self.src_table.drop_sql())
            conn.query(self.src_table.create_sql())
        with self.target_conn_builder() as conn:
            conn.query(self.target_table.drop_sql())
            conn.query(self.target_table.create_sql())

    def _observe_from_offsets(self):
        """
        Runs an OBSERVE query from the given offsets.
        """
        print(f"Starting observation from offsets: {self.offsets}")
        num_target_cols = len(self.target_table.columns)
        insert_query = f"INSERT INTO {self.target_table.name} VALUES ({ ','.join(['{}',] * num_target_cols)})"
        delete_query = f"DELETE FROM {self.target_table.name} WHERE internal_id = {{}}"
        col_assignment = ", ".join([f"{col.name} = {{}}" for col in self.src_table.columns])
        target_table_name = self.target_table.name
        update_query = f"UPDATE {target_table_name} SET {col_assignment} WHERE internal_id = {{}}"

        with self.target_conn_builder() as target_db:
            target_db_cursor = target_db.cursor()

            with ObserveQuery(
                db_name=self.src_db,
                conn_builder=self.src_conn_builder,
                tables=[self.src_table],
                offsets=self.offsets,
                timeout=int(self._pause_every_n_seconds),
            ) as observation:
                txn_iter = TransactionIterator(self.partition_count, observation)
                for txn in txn_iter:
                    target_db_cursor.execute("BEGIN")
                    self._records_per_iterations += len(txn.records)

                    if self._verbose:
                        print(
                            f"records={len(txn.records)} partition_id={txn.partition_id} "
                            f"offset={txn.begin_offset}:{txn.commit_offset}"
                        )
                    for record in txn.records:
                        if record.record_type == "Insert":
                            # Insert the row one by one into the target DB
                            query = insert_query.format(*([record.internal_id] + list(record.data)))
                        elif record.record_type == "Delete":
                            # Delete the row from the target DB matching the internal ID
                            query = delete_query.format(record.internal_id)
                        elif record.record_type == "Update":
                            # Update the row in the target DB matching the internal ID
                            query = update_query.format(*(list(record.data) + [record.internal_id]))
                        else:
                            continue

                        if self._verbose:
                            print(query)
                        target_db_cursor.execute(query)
                        target_db_cursor.fetchall()
                    target_db_cursor.execute("COMMIT")
                    self.offsets[txn.partition_id] = txn.commit_offset

    def run(self):
        """
        Runs the replication process for the table.
        """
        last_callback_on = time()
        while not self._stop_token.is_set():
            try:
                self._records_per_iterations = 0
                self._observe_from_offsets()
            except MySQLdb.Error as e:
                if e.args[0] in ALLOWED_KILL_ERROR_CODES:
                    print(f"Got KILL error={e}, continuing..")
                else:
                    raise e

            if time() - last_callback_on > self._pause_every_n_seconds:
                print("Pausing observations...")
                self._callback_between_observations(self._records_per_iterations)
                last_callback_on = time()

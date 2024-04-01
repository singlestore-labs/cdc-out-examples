#!/usr/bin/env python3
from MySQLdb import Connection

from cdc.connection_builder import ConnectionBuilder
from cdc.replication import TableReplication
from cdc.table_definition import TableDefinition

import argparse

ONE_GIGABYTE: int = 1024 ** 3

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-db', type=str, default='x_db', nargs='?', help='Name of the source database')
    parser.add_argument('--target-db', type=str, default='x_db_target', help='Name of the target database')
    parser.add_argument('--source-table', type=str, default='x_table', nargs='?', help='Name of the source table')
    parser.add_argument('--target-table', type=str, default='x_table_replicated', nargs='?',
                        help='Name of the replicated table')
    parser.add_argument("--verbose", help="Increase verbosity", action="store_true")
    return parser.parse_args()

# Source DB info
def conn_builder_source(db: str | None = None) -> ConnectionBuilder:
    return ConnectionBuilder(host="127.0.0.1", port=3306, user="root", passwd=None, db=db)

def conn_builder_target(db: str | None = None) -> ConnectionBuilder:
    return ConnectionBuilder(host="127.0.0.1", port=3306, user="root", passwd=None, db=db)


def setup_db(conn: Connection, db: str) -> None:
    print(f"Setting up DB {db}")
    """One-time setup for the DB definitions & global vars"""
    conn.query(f"CREATE DATABASE IF NOT EXISTS {db}")
    conn.query(f"USE {db}")
    conn.query("DROP ALL FROM PLANCACHE")

    # Prevent the DB from making new snapshots
    conn.query(f"set global memsql_snapshot_trigger_size={10 * ONE_GIGABYTE}")
    conn.query(f"set global snapshot_trigger_size={10 * ONE_GIGABYTE}")
    print(f"DB {db} setup done.")


if __name__ == '__main__':
    args = parse_args()

    source_db_name = args.source_db
    source_db_conn = conn_builder_source(source_db_name)()
    setup_db(source_db_conn, source_db_name)

    target_db_name = args.target_db
    target_db_conn = conn_builder_target(target_db_name)()
    setup_db(target_db_conn, target_db_name)

    source_table_name = args.source_table
    target_table_name = args.target_table

    # Create a table definition for the source table
    table_def = TableDefinition(source_table_name).column(name="foo", sql_type="int", nullable=False)
    print("Table definition:", table_def.create_sql())

    src_conn_builder = conn_builder_source(source_db_name)
    target_conn_builder = conn_builder_target(target_db_name)

    print("Setting up replication...")
    replication = TableReplication(
        src_db=source_db_name,
        src_conn_builder=src_conn_builder,
        target_db=target_db_name,
        target_conn_builder=target_conn_builder,
        table=table_def,
        target_table_name=target_table_name,
        pause_every_n_seconds=3600,
        on_pause=lambda _: None,
        verbose=args.verbose,
    )
    print("Starting replication...")
    replication.run()
    print("Replication done.")

    pass

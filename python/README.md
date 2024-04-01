# Python code example for CDC out with SingleStore v8.5+

## Example

This example demonstrates how to use the SingleStore CDC out feature to replicate data from a given table on a source
database to a mirrored table on a target database.
This is done using OBSERVE queries to capture transactional changes
on the source database & relaying those changes to the target database within a new transaction.

The example uses the MySQL Connector/Python library
to connect to the SingleStore database and the MySQLClient library to connect to the MySQL database.

## Prerequisites
- SingleStore v8.5+
- Python 3.11+
- Poetry
- MySQL Connector/Python
- MySQLClient

alternatively a docker environment can be built from the provided `Dockerfile` with:
```shell
docker build . -f Dockerfile -t singlestore-cdc-out-python
```

## Setup

The project uses poetry for dependency management. To install the dependencies, run:
```shell
poetry install
```
or 
```shell
poetry shell
```

## Running the application

To run the application, execute the following command:
```shell
poetry run python3 main.py <your args>
```
or from within a valid python environment (e.g. a virtualenv/poetry shell):
```shell
python3 main.py <your args>
```

The `--help` section should provide a list of configuration options for the table-based replication example:
```shell
usage: main.py [-h] [--source-db [SOURCE_DB]] [--target-db TARGET_DB]
               [--source-table [SOURCE_TABLE]] [--target-table [TARGET_TABLE]]
               [--verbose]

options:
  -h, --help            show this help message and exit
  --source-db [SOURCE_DB]
                        Name of the source database
  --target-db TARGET_DB
                        Name of the target database
  --source-table [SOURCE_TABLE]
                        Name of the source table
  --target-table [TARGET_TABLE]
                        Name of the replicated table
  --verbose             Increase verbosity
```




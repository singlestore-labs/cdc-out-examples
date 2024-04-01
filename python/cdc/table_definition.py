from enum import Enum
from typing import Self


def random_word(length):
    import random
    import string

    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


class ColumnDefinition:
    """A descriptive simplified version of a column definition within a table"""

    def __init__(
        self,
        name: str,
        sql_type: str,
        nullable: bool = False,
        primary_key: bool = False,
        shard_key: bool = False,
    ):
        self.name: str = name  # The name of this column
        self.sql_type: str = sql_type  # The SQL type of this column
        self.is_nullable: bool = nullable  # Whether this column is nullable
        self.primary_key: bool = primary_key  # Mark this field as the primary key of the table
        self.shard_key: bool = shard_key  # Mark this field as part of the shard key of the table

        assert self.name != "", f"The name of column {self.name} cannot be empty"
        assert self.sql_type != "", f"The type of column {self.name} cannot be empty"

    def create_sql(self) -> str:
        """Returns a column definition for this column"""
        return (
            f"{self.name} {self.sql_type} "
            f"{'NULL' if self.is_nullable else 'NOT NULL'} "
            f"{'PRIMARY KEY' if self.primary_key else ''}"
        )


class TableType(Enum):
    ROWSTORE = 1
    COLUMNSTORE = 2

    @property
    def sql_type(self) -> str:
        match self:
            case TableType.COLUMNSTORE:
                return ""
            case TableType.ROWSTORE:
                return "ROWSTORE"

    @property
    def name(self) -> str:
        match self:
            case TableType.COLUMNSTORE:
                return "columnstore"
            case TableType.ROWSTORE:
                return "rowstore"


ALL_TABLE_TYPES = [TableType.ROWSTORE, TableType.COLUMNSTORE]


class TableDefinition:
    """A descriptive simplified version of a MySQL table definition"""

    def __init__(
        self,
        name: str | None = None,
        table_type: TableType | None = None,
        db: str | None = None,
    ):
        self.db = db
        self.name: str = name or f"table_{random_word(8)}"
        self.table_type: TableType = table_type or TableType.ROWSTORE
        self.columns: list[ColumnDefinition] = []

    def rowstore(self) -> Self:
        """Marks this table as a rowstore table"""
        self.table_type = TableType.ROWSTORE
        return self

    def columnstore(self) -> Self:
        """Marks this table as a columnstore table"""
        self.table_type = TableType.COLUMNSTORE
        return self

    def column(
        self,
        name: str,
        sql_type: str,
        nullable: bool = False,
        primary_key: bool = False,
        shard_key: bool = False,
    ) -> Self:
        """Adds a new column definition"""
        self.columns.append(
            ColumnDefinition(
                name=name,
                sql_type=sql_type,
                nullable=nullable,
                primary_key=primary_key,
                shard_key=shard_key,
            )
        )
        return self

    def copy_column(self, column: ColumnDefinition) -> Self:
        """Adds a new column definition"""
        self.columns.append(column)
        return self

    def _verify(self):
        """Verifies that this table definition is valid"""
        assert len(self.columns) > 0, f"Table {self.name} has no columns"
        assert (
            sum(1 if c.primary_key else 0 for c in self.columns if c.primary_key) <= 1
        ), f"Table {self.name} can only have one primary key"
        for c in self.columns:
            assert (
                len([other for other in self.columns if c.name == other.name]) == 1
            ), f"Table {self.name} contains more than one column named {c.name}"

    def drop_sql(self) -> str:
        """Returns a DROP TABLE statement for this table"""
        self._verify()
        return f"DROP TABLE IF EXISTS x_db.{self.name}"

    def create_sql(self) -> str:
        """Returns a CREATE TABLE statement for this table"""
        self._verify()

        table_name = f"{self.name}" if self.db is None else f"{self.db}.{self.name}"
        columns = [c.create_sql() for c in self.columns]
        shard_key_names = [col.name for col in self.columns if col.shard_key]
        if len(shard_key_names) > 0:
            columns.append(f"shard key ({', '.join(shard_key_names)})")

        return f"CREATE {self.table_type.sql_type} TABLE {table_name} ({', '.join(columns)})"

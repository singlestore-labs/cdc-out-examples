from datetime import timedelta
from typing import Any, Callable, List

type_check_function = Callable[[Any], bool]
sql_value = int | str | float | bytes | None | type_check_function
equivalence_function = Callable[[sql_value, sql_value], bool]
sql_filter_function = Callable[[str], str]


def quote_value(value: sql_value) -> sql_value:
    return f"'{value}'" if isinstance(value, str) else value


class SQLType:
    def __init__(self, sql_type: str,
                 initial_value: sql_value,
                 updated_value: sql_value,
                 quoted: bool = True,
                 deleted_value: sql_value = None,
                 sql_eq: sql_filter_function | None = None,
                 is_initial: type_check_function | None = None,
                 is_updated: type_check_function | None = None, ):
        self.sql_type: str = sql_type
        self.quoted: bool = quoted
        self._initial_value: sql_value = initial_value
        self._updated_value: sql_value = updated_value
        self._deleted_value: sql_value = deleted_value
        # Filter string to use in SQL WHERE clause
        self.sql_eq: sql_filter_function | None = sql_eq
        # Equality function(s) to use in Python
        self.is_initial: type_check_function | None = is_initial
        self.is_updated: type_check_function | None = is_updated

    @property
    def initial_value(self) -> sql_value:
        return quote_value(self._initial_value) if self.quoted else self._initial_value

    @property
    def updated_value(self) -> sql_value:
        return quote_value(self._updated_value) if self.quoted else self._updated_value

    @property
    def raw_initial_value(self) -> sql_value:
        return self._initial_value

    @property
    def raw_updated_value(self) -> sql_value:
        return self._updated_value

    @property
    def sql_type_name(self) -> str:
        return self.sql_type.split('(')[0]

    def where_initial(self) -> str:
        return f"{self.sql_type} = {self.initial_value}" if self.sql_eq is None else self.sql_eq

    def where_updated(self) -> str:
        return f"{self.sql_type} = {self.updated_value}" if self.sql_eq is None else self.sql_eq

    def deleted_value(self, is_nullable: bool) -> sql_value:
        """Return the deleted value for this type, or None if not nullable."""
        # DB-65478: projected values are not properly nullable
        return None if is_nullable else self._deleted_value

    def __str__(self):
        return (f"BasicSqlType(sql_type={self.sql_type},"
                f"initial_value={self.initial_value}, "
                f"updated_value={self.updated_value}, "
                f"deleted_value={self._deleted_value}), ")

    def __repr__(self):
        return self.__str__()


def assert_observed_values(values: List[Any], expected_values: List[sql_value]):
    assert len(values) == len(expected_values), (f"Expected {len(expected_values)} values, but got {len(values)}\n"
                                                 f"{expected_values} vs {values}")
    for value, expected in zip(values, expected_values):
        if callable(expected):
            assert expected(value), f"Expected <{value}> to pass {expected}"
        else:
            assert value == expected, f"Expected <{value}>({type(value)}) to equal <{expected}>({type(expected)})"


def _contains_binary_str(expected: bytes) -> Callable[[bytes], bool]:
    return lambda x: expected in x

class BinarySQLType(SQLType):
    """SQLType, which contains binary/blob data."""
    def __init__(self, sql_type: str, fixed_size: int | None = None, deleted_value: bytes | None = b""):
        name = sql_type.split('(')[0]
        initial_value = f"Hello {name}"
        updated_value = f"Goodbye {name}"
        if fixed_size is not None:
            initial_value = initial_value.zfill(fixed_size)
            updated_value = updated_value.zfill(fixed_size)
        super().__init__(sql_type, initial_value=initial_value,
                         updated_value=updated_value,
                         deleted_value=deleted_value,
                         is_initial=_contains_binary_str(initial_value.encode('utf-8')),
                         is_updated=_contains_binary_str(updated_value.encode('utf-8')),
                         sql_eq=lambda col: f"{col} = \"{initial_value}\"")


BASIC_SQL_TYPES = \
    [
        SQLType(sql_type='tinyint', initial_value=42, updated_value=12),
        SQLType(sql_type='smallint', initial_value=43, updated_value=13),
        SQLType(sql_type='mediumint', initial_value=44, updated_value=14),
        SQLType(sql_type='int', initial_value=45, updated_value=15),
        SQLType(sql_type='year', initial_value='2009-02-13', updated_value='2010-02-13',
                is_initial=lambda x: x == 2009,
                is_updated=lambda x: x == 2010,
                sql_eq=lambda col: f"{col} = 2009", ),
        SQLType(sql_type='integer', initial_value=46, updated_value=16),
        SQLType(sql_type='bigint', initial_value=47, updated_value=17),
        SQLType(sql_type='double', initial_value=48.0, updated_value=18.0, deleted_value=0.0),
        SQLType(sql_type='real', initial_value=49.0, updated_value=19.0, deleted_value=0.0),
        SQLType(sql_type='float', initial_value=50.0, updated_value=20.0, deleted_value=0.0),
        SQLType(sql_type='decimal(10, 5)', initial_value=51.0, updated_value=21.0, deleted_value=0.0),
        SQLType(sql_type='time', initial_value='2020-01-01 05:06', updated_value='2020-01-01 05:07',
                is_initial=lambda x: isinstance(x, timedelta),
                is_updated=lambda x: isinstance(x, timedelta)),
        SQLType(sql_type='date', initial_value='2019-01-01 05:07', updated_value='2020-01-01 05:07',
                sql_eq=lambda col: f"YEAR({col}) = 2019",
                is_initial=lambda x: x.year == 2019,
                is_updated=lambda x: x.year == 2020),
        SQLType(sql_type='datetime', initial_value='2019-01-01 05:09', updated_value='2020-01-01 05:08',
                sql_eq=lambda col: f"YEAR({col}) = 2019",
                is_initial=lambda x: x.year == 2019,
                is_updated=lambda x: x.year == 2020),
        SQLType(sql_type='timestamp', initial_value='2019-01-01 05:10', updated_value='2020-01-01 05:11',
                sql_eq=lambda col: f"YEAR({col}) = 2019",
                is_initial=lambda x: x.year == 2019,
                is_updated=lambda x: x.year == 2020),
        SQLType(sql_type='char(4)', initial_value='a', updated_value='b'),
        SQLType(sql_type='varchar(50)', initial_value='Hello world', updated_value='Hello world BIS', deleted_value=""),
        BinarySQLType(sql_type='binary(50)', fixed_size=50, deleted_value=None),
        BinarySQLType(sql_type='varbinary(50)'),
        BinarySQLType(sql_type='tinyblob'),
        BinarySQLType(sql_type='mediumblob'),
        BinarySQLType(sql_type='blob'),
        BinarySQLType(sql_type='longblob'),
        SQLType(sql_type='tinytext', initial_value='Hello ttworld', updated_value='Hello ttworld BIS',
                deleted_value=""),
        SQLType(sql_type='mediumtext', initial_value='Hello mtworld', updated_value='Hello mtworld BIS',
                deleted_value=""),
        SQLType(sql_type='text', initial_value='Hello tworld', updated_value='Hello tworld BIS', deleted_value=""),
        SQLType(sql_type='longtext', initial_value='Hello ltworld', updated_value='Hello ltworld BIS',
                deleted_value=""),
        SQLType(sql_type='bit', initial_value=b'1', updated_value=b'0',
                sql_eq=lambda col: f"{col} = '1'",
                is_initial=lambda x: x == b'\x00\x00\x00\x00\x00\x00\x00\x01',
                is_updated=lambda x: x == b'\x00\x00\x00\x00\x00\x00\x00\x00'),
    ]

EXTENDED_SQL_TYPES = \
    [
        SQLType(sql_type='enum("a", "b", "c")', initial_value='a', updated_value='b'),
        SQLType(sql_type='set("a", "b", "c")', initial_value='a,b', updated_value='b,c'),
        SQLType(sql_type="json", initial_value='{"a":1}', updated_value='{"b":2}'),
        SQLType(sql_type="bson", quoted=False, initial_value='0x0500000000 :> BSON',
                is_initial=_contains_binary_str(b'\x05\x00\x00\x00\x00'),
                updated_value='0x0108 :> BSON',
                is_updated=_contains_binary_str(b'\x01\x08')),
        SQLType(sql_type="GEOGRAPHY",
                initial_value='POLYGON((1 1,2 1,2 2, 1 2, 1 1))',
                is_initial=lambda x: x == (
                                     'POLYGON((1.00000000 1.00000000, 2.00000000 1.00000000, 2.00000000 2.00000000, '
                                     '1.00000000 2.00000000, 1.00000000 1.00000000))'),
                updated_value='POLYGON((5 1,6 1,6 2,5 2,5 1))',
                is_updated=lambda x: x == (
                    'POLYGON((5.00000000 1.00000000, 6.00000000 1.00000000, 6.00000000 2.00000000, '
                    '5.00000000 2.00000000, 5.00000000 1.00000000))')),
        SQLType(sql_type="GEOGRAPHYPOINT", initial_value='POINT(50.01 40.01)',
                is_initial=lambda x: x == "POINT(50.01000000 40.00999998)",
                updated_value='POINT(50.02 40.02)',
                is_updated=lambda x: x == "POINT(50.01999998 40.02000002)"),
        SQLType(sql_type="VECTOR(3)", initial_value='[0.1,0.2,0.3]',
                is_initial=lambda x: x == '[0.100000001,0.200000003,0.300000012]',
                updated_value='[0.4,0.5,0.6]',
                is_updated=lambda x: x == '[0.400000006,0.5,0.600000024]',
                sql_eq=lambda col: f"VECTOR_ELEMENTS_SUM({col}) BETWEEN 0.0 and 1.0"),
    ]

ALL_SQL_TYPES = BASIC_SQL_TYPES + EXTENDED_SQL_TYPES

def find_sql_type(sql_name: str) -> SQLType:
    for sql_type in ALL_SQL_TYPES:
        if sql_type.sql_type_name == sql_name:
            return sql_type
    raise ValueError(f"Unknown SQL type: {sql_name}")

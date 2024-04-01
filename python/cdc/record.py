from binascii import hexlify


class CdcOffset:
    """Deserialized version of a CDC offset"""

    def __init__(self, data):
        assert data is not None, "Expected an offset, but got None"
        assert len(data) == 24, f"Expected an offset with 24 bytes got {len(data)}"
        self._data = data

    def __str__(self):
        return hexlify(self._data).decode("utf-8")

    def __repr__(self):
        return self.__str__()


class CdcRecord:
    """Deserialized version of a CDC record"""

    def __init__(self, data):
        assert data is not None, "Expected a record, but got None"
        assert len(data) >= 7, "Expected a record with at least 7 aux columns"
        self.offset = data[0]
        self.partition_id = data[1]
        self.record_type = data[2]
        self.table = data[3]
        self.tx_id = data[4]
        self.tx_partitions = data[5]
        self.internal_id = data[6]
        self.data = data[7:]

    def _data_equals(self, other):
        # DB-65478: projected values are not properly nullable
        def defaulted_equals(a, b):
            """Return true if a and b are equal, or if one is None and the other is the default value for its type"""
            return a == b or (a is None and b == type(b)()) or (a == type(a)() and b is None)

        return len(self.data) == len(other.data) and all(defaulted_equals(a, b) for a, b in zip(self.data, other.data))

    def __eq__(self, other):
        return (
            self.offset == other.offset
            and self.partition_id == other.partition_id
            and self.record_type == other.record_type
            and self.table == other.table
            and self.tx_id == other.tx_id
            and self.tx_partitions == other.tx_partitions
            and self.internal_id == other.internal_id
            and self._data_equals(other)
        )

    def __str__(self):
        return (
            f"Record("
            f"offset={CdcOffset(self.offset)}, partition_id={self.partition_id}, record_type={self.record_type}, "
            f"table={self.table}, tx_id={hexlify(self.tx_id)!r}, tx_partitions={self.tx_partitions}, "
            f"internal_id={self.internal_id} data={self.data})"
        )

    def __repr__(self):
        return self.__str__()

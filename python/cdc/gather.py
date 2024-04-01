from pprint import pformat
from typing import List

import MySQLdb.cursors

from cdc.observe_query import CdcRecord
from cdc.record import CdcOffset


class ObservedTransaction:
    """A transaction as observed from the CDC stream."""

    def __init__(self, begin_txn: CdcRecord):
        assert (
            begin_txn.record_type == "BeginTransaction" or begin_txn.record_type == "BeginSnapshot"
        ), f"Expected a begin record, got {begin_txn.record_type}"
        self.begin_offset: str | bytes = begin_txn.offset
        self.commit_offset: str | bytes = "invalid"
        self.partition_id: int = begin_txn.partition_id
        self.transaction_id: str = begin_txn.tx_id
        self.transaction_partitions: int = begin_txn.tx_partitions
        self.records: List[CdcRecord] = []
        self.is_complete: bool = False

    @property
    def is_snapshot(self) -> bool:
        return False

    def __repr__(self):
        return (
            f"ObservedTransaction(partition_id={self.partition_id}{' snapshot ' if self.is_snapshot else ' '}"
            f"transaction_id={self.transaction_id!r} "
            f"partitions={self.transaction_partitions} "
            f"complete={self.is_complete} "
            f"begin_offset={CdcOffset(self.begin_offset)} "
            f"commit_offset={CdcOffset(self.commit_offset)})\nrecords={pformat(self.records)}"
        )

    @property
    def is_empty(self) -> bool:
        """Return True if the transaction has no records."""
        return len(self.records) == 0

    def append(self, record: CdcRecord) -> None:
        """Add a record to the transaction."""
        assert record.tx_id == self.transaction_id, f"Expected match for TxId but got record={record} vs txn={self}"
        assert record.record_type not in (
            "BeginTransaction",
            "BeginSnapshot",
        ), "Expected a record, not a transaction start"
        assert (
            record.partition_id == self.partition_id
        ), f"Expected partition_id {self.partition_id}, but got {record.partition_id}"
        assert not self.is_complete, f"Adding {record}, but the transaction is already complete"

        self.records.append(record)

    def commit(self, commit_txn: CdcRecord) -> None:
        """Commit the transaction."""
        if self.is_snapshot:
            if self.transaction_id != commit_txn.tx_id:
                # We can have sub-transactions within the snapshot, skip the txn markers
                assert (
                    commit_txn.record_type == "CommitTransaction"
                ), f"Expected a commit record, not a transaction start {commit_txn}"
                return
            else:
                assert commit_txn.record_type == "CommitSnapshot", f"Expected a commit record, not {commit_txn}"
        else:
            assert (
                commit_txn.tx_id == self.transaction_id
            ), f"Expected TxId match for commit={commit_txn} but got txn={self}"

        assert commit_txn.partition_id == self.partition_id, "Expected the same partition_id in the commit record"
        assert (
            commit_txn.tx_partitions == self.transaction_partitions
        ), "Expected the same tx_partitions in the commit record"
        self.commit_offset = commit_txn.offset
        self.is_complete = True


class ObservedSnapshot:
    """A snapshot as observed from the CDC stream."""

    def __init__(self, begin_snapshot: CdcRecord):
        assert (
            begin_snapshot.record_type == "BeginSnapshot"
        ), f"Expected a begin snapshot record, got {begin_snapshot.record_type}"
        self.begin_offset: str | bytes = begin_snapshot.offset
        self.commit_offset: str | bytes = "invalid"
        self.partition_id: int = begin_snapshot.partition_id
        self.transaction_id: str = begin_snapshot.tx_id
        self.transaction_partitions: int = begin_snapshot.tx_partitions
        self.records: List[CdcRecord] = []
        self.is_complete: bool = False

    @property
    def is_snapshot(self) -> bool:
        return True

    def __repr__(self):
        return (
            f"ObservedSnapshot(partition_id={self.partition_id}{' snapshot ' if self.is_snapshot else ' '}"
            f"transaction_id={self.transaction_id!r} "
            f"partitions={self.transaction_partitions} "
            f"complete={self.is_complete} "
            f"begin_offset={CdcOffset(self.begin_offset)} "
            f"commit_offset={CdcOffset(self.commit_offset)})\nrecords={pformat(self.records)}"
        )

    @property
    def is_empty(self) -> bool:
        """Return True if the snapshot has no records."""
        return len(self.records) == 0

    def append(self, record: CdcRecord) -> None:
        """Add a record to the snapshot."""
        assert record.record_type not in (
            "BeginTransaction",
            "BeginSnapshot",
            "CommitTransaction",
            "CommitSnapshot",
        ), "Expected a record, not a begin/commit"
        assert (
            record.partition_id == self.partition_id
        ), f"Expected partition_id {self.partition_id}, but got {record.partition_id}"
        assert not self.is_complete, f"Adding {record}, but the snapshot is already complete"
        self.records.append(record)

    def commit(self, commit_txn: CdcRecord) -> None:
        """Commit the snapshot."""
        assert (
            commit_txn.tx_id == self.transaction_id
        ), f"Expected TxId match for commit={commit_txn} but got txn={self}"
        assert commit_txn.partition_id == self.partition_id, "Expected the same partition_id in the commit record"
        assert (
            commit_txn.tx_partitions == self.transaction_partitions
        ), "Expected the same tx_partitions in the commit record"
        self.commit_offset = commit_txn.offset
        self.is_complete = True


class TransactionIterator:
    """Reconstructs transactions from a (potential) multi-partition stream of records."""

    def __init__(self, partitions: int, cursor: MySQLdb.cursors.Cursor, skip_empty: bool = True):
        # DB cursor to fetch records from
        self._observation = cursor
        # List of records within a transaction, per partition
        self._txn_records: List[ObservedSnapshot | ObservedTransaction | None] = [
            None,
        ] * partitions
        # The offset of the last record in a transaction, per partition
        self._txn_offsets: List[str | bytes | None] = [
            None,
        ] * partitions
        self._is_singlebox = partitions == 1
        self._skip_empty = skip_empty

    def __iter__(self):
        return self

    def update_cursor(self, cursor: MySQLdb.cursors.Cursor) -> None:
        self._observation = cursor

    def _process_record(self, next_record: CdcRecord) -> ObservedTransaction | ObservedSnapshot | None:
        partition_id: int = 0 if self._is_singlebox else next_record.partition_id
        self._txn_offsets[partition_id] = next_record.offset

        if next_record.record_type == "BeginSnapshot":
            # Start of a new snapshot
            assert self._txn_records[partition_id] is None, (
                f"Expected a new snapshot, but we had a previous" f" record={self._txn_records[partition_id]}"
            )
            self._txn_records[partition_id] = ObservedSnapshot(next_record)
            return None
        elif next_record.record_type == "BeginTransaction":
            # Start of a new transaction
            assert (
                self._txn_records[partition_id] is None
                or not self._txn_records[partition_id].is_snapshot  # type: ignore[reportOptionalMemberAccess, union-attr]
                or self._txn_records[partition_id].is_complete  # type: ignore[reportOptionalMemberAccess, union-attr]
            ), "Got a new transaction, no valid prior state"
            self._txn_records[partition_id] = ObservedTransaction(next_record)
            return None
        elif next_record.record_type in ("CommitTransaction", "CommitSnapshot"):
            # End of a transaction
            if self._txn_records[partition_id] is None:
                # Partial txn, just skip
                return None
            self._txn_records[partition_id].commit(next_record)  # type: ignore[reportOptionalMemberAccess, union-attr]

            if self._txn_records[partition_id].is_empty and self._skip_empty:  # type: ignore[reportOptionalMemberAccess, union-attr]
                return None
            return self._txn_records[partition_id]
        else:
            # Record content
            if self._txn_records[partition_id] is None:
                # Partial txn, just skip
                return None
            self._txn_records[partition_id].append(next_record)  # type: ignore[reportOptionalMemberAccess, union-attr]
            return None

    def __next__(self):
        return self.next()

    def next(self):
        while True:
            data = self._observation.fetchone()
            if not data:
                raise StopIteration
            next_record = CdcRecord(data)

            txn_opt = self._process_record(next_record)
            if txn_opt is not None:
                return txn_opt

    @property
    def offsets(self) -> List[str | bytes | None]:
        return self._txn_offsets

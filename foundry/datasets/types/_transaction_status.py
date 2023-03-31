from enum import EnumMeta


class TransactionStatus(EnumMeta):
    """The status of a Transaction."""

    ABORTED = "ABORTED"
    COMMITTED = "COMMITTED"
    OPEN = "OPEN"

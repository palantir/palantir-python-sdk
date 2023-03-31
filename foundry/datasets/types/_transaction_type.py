from enum import EnumMeta


class TransactionType(EnumMeta):
    """The type of a Transaction."""

    APPEND = "APPEND"
    UPDATE = "UPDATE"
    SNAPSHOT = "SNAPSHOT"
    DELETE = "DELETE"

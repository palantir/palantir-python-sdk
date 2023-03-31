from enum import EnumMeta


class TableExportFormat(EnumMeta):
    """Format for tabular dataset export."""

    ARROW = "ARROW"
    CSV = "CSV"

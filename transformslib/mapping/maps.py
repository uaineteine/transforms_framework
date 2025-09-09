from typing import NamedTuple
from enum import IntEnum

class TransformTableColumnMapping(NamedTuple):  # unused
    """
    Represents a custom column-level mapping between input and output tables
    during a transformation.

    Attributes:
        from_table: Name of the source table.
        from_column_name: Name of the source column.
        to_table: Name of the destination table.
        to_column_name: Name of the destination column.
    """
    from_table: str
    from_column_name: str
    to_table: str
    to_column_name: str

class TableColumnMapping(NamedTuple):  # unused
    """
    Represents a lineage mapping of a column from one versioned table to another.

    Attributes:
        from_table: Name of the source table.
        from_table_ver: Version of the source table.
        from_column_name: Name of the source column.
        to_table: Name of the destination table.
        to_table_ver: Version of the destination table.
        to_column_name: Name of the destination column.
    """
    from_table: str
    from_table_ver: int
    from_column_name: str
    to_table: str
    to_table_ver: int
    to_column_name: str

class TableMapping(NamedTuple):  # unused
    """
    Represents a mapping between two versioned tables.

    Attributes:
        from_table: Name of the source table.
        from_table_ver: Version of the source table.
        to_table: Name of the destination table.
        to_table_ver: Version of the destination table.
    """
    from_table: str
    from_table_ver: int
    to_table: str
    to_table_ver: int

class TransformColumnMappingType(IntEnum):  # unused
    """
    Enum defining default column mapping behavior during transformations.

    Values:
        ALL_INPUT_COLUMNS (-2): All columns from input tables map to same-name columns in output tables by default.
        BREAK_LINEAGE (-1): No default mappings; lineage is broken. 0 and above: Only columns from the specified input table index are mapped by default.
    """
    ALL_INPUT_COLUMNS = -2
    BREAK_LINEAGE = -1

# Commentary for context:
# - TransformTableColumnMapping allows overriding default column lineage behavior.
#   If from_table/from_column_name are None, the column is newly created.
#   If to_table/to_column_name are None, the column is deleted.
#   All input columns must be explicitly defined when using this.
#
# - TableColumnMapping tracks column-level lineage across table versions.
#   Useful for understanding how specific columns evolve.
#
# - TableMapping tracks table-level lineage across versions.
#
# - TransformColumnMappingType controls default behavior:
#   -2 = map all input columns by name,
#   -1 = break lineage (no default mappings),
#    0+ = map only from specified input table index.
#
# - TODO: Consider attaching transform name to mappings.
# - NOTE: Applying PySpark transforms directly to metaframes may not be ideal.
#   See template_custom_transform for alignment.
# - Hi Dan S — apologies if any class usage is off!

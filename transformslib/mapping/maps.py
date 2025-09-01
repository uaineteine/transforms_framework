from typing import NamedTuple
from enum import IntEnum

# this gives us the ability to add extra mappings to column lineage in addition to the default behaviour given by column_mapping_type
# from_table = None and from_column_name = None mean column created from no table
# to_table = None and to_column_name = None mean column deleted from table
# any TransformTableColumnMapping overwrite the default behaviour of the from_column_name mapping: all mappings for any
# provided input columns must be explicitly defined
class TransformTableColumnMapping(NamedTuple):
    from_table: str
    from_column_name: str
    to_table: str
    to_column_name: str

# NOTE: Hi Dan S I'm directly applying pyspark transforms to the metaframes i think im doing it wrong, but seems to align with template_custom_transform hehe
# also apologies in advance if i muck up the usage of any of ur classes
# TODO: can potentially attach transform name to the mapping as well mayhaps!
class TableColumnMapping(NamedTuple):
    from_table: str
    from_table_ver: int
    from_column_name: str
    to_table: str
    to_table_ver: int
    to_column_name: str

class TableMapping(NamedTuple):
    from_table: str
    from_table_ver: int
    to_table: str
    to_table_ver: int

# -2 all columns from input tables map to all corresponding same named columns in output tables by default
# -1 no mappings for any columns by default
# integer values 0+ mean take columns from that table only by default
# note that all links must be consistent with actual input/output table columns after override_column_maps are applied
class TransformColumnMappingType(IntEnum):
    ALL_INPUT_COLUMNS = -2
    BREAK_LINEAGE = -1

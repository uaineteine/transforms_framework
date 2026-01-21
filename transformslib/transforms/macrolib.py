from typing import Union, TYPE_CHECKING

from transformslib.engine import get_engine, get_spark

from transformslib.templates.pathing import apply_formats

if TYPE_CHECKING:
    from transformslib.tables.collections.collection import TableCollection

from transformslib.transforms.pipeevent import PipelineEvent
from .base import MacroTransform, printwidth
from .atomiclib import *
import os

# Get synthetic variable name from environment variable
SYNTHETIC_VAR = os.environ.get("TNSFRMS_SYN_VAR", "synthetic")
PERSON_ID_VAR = os.environ.get("TNSFRMS_ID_VAR", "person_id")

class Macro:
    """
    A wrapper class for applying a macro transformation to a collection of tables
    and logging the transformation metadata.

    :param macro_transform: A MacroTransform object containing the transformation logic.
    :type macro_transform: MacroTransform
    :param input_tables: A collection of input tables to be transformed.
    :type input_tables: TableCollection
    :param output_tables: List of names of output tables.
    :type output_tables: list[str]
    :param input_variables: List of input variable names used in the transformation.
    :type input_variables: list[str]
    :param output_variables: List of output variable names produced by the transformation.
    :type output_variables: list[str]
    """

    def __init__(self,
                 macro_transform: MacroTransform,
                 input_tables: "TableCollection",
                 output_tables: list[str],
                 input_variables: list[str],
                 output_variables: list[str]):
        self.macros = macro_transform
        self.input_tables = input_tables
        self.output_tables = output_tables
        self.input_variables = input_variables
        self.output_variables = output_variables
        
        #get this from environment variable
        #TNSFRMS_LOG_LOC="jobs/prod/job_{job_id}/treatments.json"
        MACRO_LOG_LOC = os.environ.get("TNSFRMS_LOG_LOC", "jobs/prod/job_{job_id}/treatments.json")
        self.macro_log_loc = apply_formats(MACRO_LOG_LOC)

    def apply(self, **kwargs) -> "TableCollection":
        """
        Applies the macro transformation to the input tables and logs the operation.

        :return: Transformed table frames.
        :rtype: TableCollection
        """
        spark = get_spark()
        
        table_names = self.input_tables.get_table_names()
        #print(table_names)
        
        return_frames = None
        
        for i, tbl in enumerate(table_names):
            return_frames = self.macros.apply(self.input_tables, spark=spark, df=tbl, **kwargs)
            
        return return_frames

    def log(self):
        """
        Logs the macro transformation metadata to a JSON file.

        Args:
            spark: SparkSession object (required for PySpark frame_type). Defaults to None.
        """
        # Create a serializable version of the object dict
        json_info = {
            'input_tables': [str(table) for table in self.input_tables.get_table_names()],
            'output_tables': self.output_tables,
            'input_variables': self.input_variables,
            'output_variables': self.output_variables,
            'macro_log_loc': self.macro_log_loc,
            'macro_name': self.macros.name,
            'macro_description': self.macros.event_description,
            'macro_type': self.macros.transform_type
        }
        log_info = PipelineEvent("macro_log", json_info, event_description="the driver macro for atomic transforms", log_location=self.macro_log_loc)
        spark = get_spark()
        log_info.log(spark=spark)

###### LIBRARY OF MACRO TRANSFORMS ######

class TopBottomCode(Macro):
    """
    A macro that applies top and bottom coding to specified variables in a table collection.
    Values above `max_value` are capped, and values below `min_value` are floored.

    :param input_tables: A collection of input tables to be transformed.
    :type input_tables: TableCollection
    :param input_variables: List of variable names to apply top/bottom coding.
    :type input_variables: list[str]
    :param max_value: Maximum allowed value; values above this will be replaced.
    :type max_value: int or float
    :param min_value: Minimum allowed value; values below this will be replaced.
    :type min_value: int or float
    """

    def __init__(self,
                 input_tables: "TableCollection",
                 input_variables: list[str],
                 max_value: Union[int, float],
                 min_value: Union[int, float]):
        transforms = []
        for var in input_variables:
            var_transforms = [
                ReplaceByCondition(
                    column=var,
                    op=">",
                    value=max_value,
                    replacement=max_value
                ),
                ReplaceByCondition(
                    column=var,
                    op="<",
                    value=min_value,
                    replacement=min_value
                )
            ]
            transforms.extend(var_transforms)

        macro = MacroTransform(
            transforms=transforms,
            Name="TopCode",
            Description="Sets maximum value on variable",
            macro_id="TopCode"
        )

        super().__init__(
            macro_transform=macro,
            input_tables=input_tables,
            output_tables=input_tables.get_table_names(),
            input_variables=input_variables,
            output_variables=input_variables
        )


class ConcatenateIDs(Macro):
    """
    A macro that concatenates two ID columns with an underscore separator.
    Creates a new column by combining the values of two specified columns using ConcatColumns transform.

    :param input_tables: A collection of input tables to be transformed.
    :type input_tables: TableCollection
    :param input_columns: List containing exactly two column names to concatenate.
    :type input_columns: list[str]
    :param output_column: Name of the new column to create with concatenated values.
    :type output_column: str
    """

    def __init__(self,
                 input_tables: "TableCollection",
                 input_columns: list[str],
                 output_column: str):
        if len(input_columns) != 2:
            raise ValueError("ConcatenateIDs requires exactly 2 input columns")
        
        # Create the ConcatColumns transform with underscore separator
        concat_transform = ConcatColumns(
            variables_to_concat=input_columns,
            sep="_"
        )

        macro = MacroTransform(
            transforms=[concat_transform],
            Name="ConcatenateIDs",
            Description="Concatenates two ID columns with underscore separator",
            macro_id="ConcatIDs"
        )

        super().__init__(
            macro_transform=macro,
            input_tables=input_tables,
            output_tables=input_tables.get_table_names(),
            input_variables=input_columns,
            output_variables=[output_column]
        )

    def apply(self, **kwargs) -> "TableCollection":
        # Override apply to specify output column name
        kwargs['output_var'] = self.output_variables[0]
        return super().apply(**kwargs)

class DropMissingIDs(Macro):
    """
    A macro that drops missing IDs from a table by removing rows with NA values in the synthetic variable.
    Uses the DropNAValues transform to remove rows with missing values.

    :param input_tables: A collection of input tables to be transformed.
    :type input_tables: TableCollection
    """

    def __init__(self,
                 input_tables: "TableCollection"):
        # Create the DropNAValues transform targeting the synthetic variable
        drop_transform = DropNAValues(
            column=SYNTHETIC_VAR
        )

        macro = MacroTransform(
            transforms=[drop_transform],
            Name="DropMissingIDs",
            Description=f"Drops missing IDs by removing rows with NA values in {SYNTHETIC_VAR}",
            macro_id="DropMissing"
        )

        super().__init__(
            macro_transform=macro,
            input_tables=input_tables,
            output_tables=input_tables.get_table_names(),
            input_variables=[SYNTHETIC_VAR],
            output_variables=[SYNTHETIC_VAR]
        )

class ApplyLegacyIDHash(Macro):
    """
    Transform class to apply specific HMAC hashing to a specified column using a secret key.
    """

    def __init__(self,
                 input_tables: "TableCollection"):
       
        syn_hash = ApplyHMAC(column=SYNTHETIC_VAR, trunclength=16)
        per_hash = ApplyHMAC(column=PERSON_ID_VAR, trunclength=16)

        macro = MacroTransform(
            transforms=[syn_hash, per_hash],
            Name="ApplyLegacyIDHash",
            Description=f"Applies HMAC hashing to {SYNTHETIC_VAR} and {PERSON_ID_VAR}",
            macro_id="ApplyLegacyIDHash"
        )

        super().__init__(
            macro_transform=macro,
            input_tables=input_tables,
            output_tables=input_tables.get_table_names(),
            input_variables=[SYNTHETIC_VAR, PERSON_ID_VAR],
            output_variables=[SYNTHETIC_VAR, PERSON_ID_VAR]
        )
        
        self.secret_key_name = "LegacyIDHash"


### IMPORT LOGIC TO DISCOVER ALL MACROS ###

def _discover_macros():
    """
    Discover all Macro subclasses in the current module.
    Returns a list of tuples (class_name, class_obj, description).
    """
    import sys
    import inspect
    current_module = sys.modules[__name__]
    macros = []
    
    for name in dir(current_module):
        obj = getattr(current_module, name)
        if (inspect.isclass(obj) and 
            issubclass(obj, Macro) and 
            obj is not Macro):
            
            # Extract description from docstring
            description = ""
            if obj.__doc__:
                lines = obj.__doc__.strip().split('\n')
                if lines:
                    description = lines[0].strip()
            
            macros.append((name, obj, description))
    
    return sorted(macros)


def listmacro():
    """
    Display all available macro transforms in a neat table format.
    """
    macros = _discover_macros()
    
    if not macros:
        print("No macro transforms found.")
        return
    
    print("\n" + "="*printwidth)
    print(" MACRO LIBRARY - Available Macro Transform Classes")
    print("="*printwidth)
    print(f" Total Macro Transforms: {len(macros)}")
    print("="*printwidth)
    
    # Calculate column widths
    max_name_width = max(len(name) for name, _, _ in macros) if macros else 10
    max_desc_width = printwidth - max_name_width - 5  # Leave space for formatting
    
    print(f"{'Macro Name':<{max_name_width}} | Description")
    print("-" * max_name_width + "-+-" + "-" * max_desc_width)
    
    for name, _, description in macros:
        # Truncate description if too long
        if len(description) > max_desc_width:
            description = description[:max_desc_width-3] + "..."
        print(f"{name:<{max_name_width}} | {description}")
    
    print("="*printwidth)
    print(" Use help(ClassName) for detailed information about any macro transform.")
    print("="*printwidth + "\n")


# Automatically discover and set up __all__ with all macros
_macros = _discover_macros()
_macro_names = [name for name, _, _ in _macros]

# Export all macros and the listmacro function
__all__ = _macro_names + ['listmacro', 'Macro']

# Show the count whenever the module is imported (but not when run as main)
if __name__ != '__main__':
    print(f"\n Macro Library: {len(_macros)} macro transforms available")
    print("   Use listmacro() to see all available macro transforms in a table format.\n")

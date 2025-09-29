from transformslib.tables.collections.collection import TableCollection 
from .base import MacroTransform, Macro, printwidth
from .atomiclib import *
from typing import Union
import os

macro_log_location = "jobs/prod/job_1/treatments.json"

# Get synthetic variable name from environment variable
SYNTHETIC_VAR = os.environ.get("TNSFRMS_SYN_VAR", "synthetic")

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
                 input_tables: TableCollection,
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
                 input_tables: TableCollection,
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


class DropMissingIDs(Macro):
    """
    A macro that drops missing IDs from a table by removing rows with NA values in the synthetic variable.
    Uses the DropNAValues transform to remove rows with missing values.

    :param input_tables: A collection of input tables to be transformed.
    :type input_tables: TableCollection
    """

    def __init__(self,
                 input_tables: TableCollection):
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

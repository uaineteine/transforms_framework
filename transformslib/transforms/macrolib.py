import json
from transformslib.tables.collections.collection import TableCollection 
from transformslib.transforms.base import MacroTransform
from transformslib.transforms.atomiclib import *
from typing import Union

macro_log_location = "events_log/job_1/treatments.json"

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
                 input_tables: TableCollection,
                 output_tables: list[str],
                 input_variables: list[str],
                 output_variables: list[str]):
        self.macros = macro_transform
        self.input_tables = input_tables
        self.output_tables = output_tables
        self.input_variables = input_variables
        self.output_variables = output_variables
        self.macro_log_loc = macro_log_location

    def apply(self, **kwargs):
        """
        Applies the macro transformation to the input tables and logs the operation.

        Args:
            **kwargs: Keyword arguments to pass to the underlying transforms.

        :return: Transformed table frames.
        :rtype: dict[str, pd.DataFrame]
        """
        return_frames = self.macros.apply(self.input_tables, **kwargs)
        self.log()
        return return_frames

    def log(self):
        """
        Logs the macro transformation metadata to a JSON file.
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
        with open(self.macro_log_loc, 'w') as f:
            json.dump(json_info, f, indent=2)

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

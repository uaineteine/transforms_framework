from typing import List, Union, Dict, Callable
import inspect

from transforms.base import TableTransform
from tables.collections.collection import TableCollection

from pyspark.sql.functions import col

def _get_lambda_source(func) -> str:
    try:
        return inspect.getsource(func).strip()
    except Exception as e:
        return f"<source unavailable: {e}>"

class DropVariable(TableTransform):
    """
    Transform class for removing one or more variables/columns from a DataFrame.
    """

    def __init__(self, variables_to_drop: Union[str, List[str]]):
        """
        Initialise a DropVariable transform.

        Args:
            variables_to_drop (Union[str, List[str]]): The name(s) of the variable(s)/column(s) to remove.
        """

        super().__init__(
            "DropVariable",
            "Removes specified variable(s) from a dataframe",
            variables_to_drop,
            "DropVar",
            testable_transform=True
        )

        print(self.target_variables)

    def error_check(self, supply_frames: TableCollection, **kwargs):
        """
        Validate that all variables to drop exist in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        missing_vars = [var for var in self.vars if var not in supply_frames[table_name].columns]
        if missing_vars:
            raise ValueError(f"Variables not found in DataFrame columns: {missing_vars}")

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Remove the specified variables from the DataFrame.
        """
        table_name = kwargs.get('df')

        supply_frames[table_name].drop(columns=self.vars)
        supply_frames[table_name].add_event(self)

        self.deleted_variables = self.vars
        self.target_tables = [table_name]

        return supply_frames

    def test(self, supply_frames: TableCollection, **kwargs) -> bool:
        """
        Test that the variables were successfully removed from the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            return False

        return all(var not in supply_frames[table_name].columns for var in self.vars)

class SubsetTable(TableTransform):
    """
    Transform class for subsetting a DataFrame to retain only specified columns.
    """

    def __init__(self, variables_to_keep: Union[str, List[str]]):
        """
        Initialise a SubsetTable transform.

        Args:
            variables_to_keep (Union[str, List[str]]): The name(s) of the variable(s)/column(s) to retain.
        """
        super().__init__(
            "SubsetTable",
            "Subsets a dataframe to retain only specified variable(s)",
            variables_to_keep,
            "SubsetTbl",
            testable_transform=True
        )

        print(self.target_variables)

    def error_check(self, supply_frames: TableCollection, **kwargs):
        """
        Validate that all variables to keep exist in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        missing_vars = [var for var in self.vars if var not in supply_frames[table_name].columns]
        if missing_vars:
            raise ValueError(f"Variables not found in DataFrame columns: {missing_vars}")

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Subset the DataFrame to retain only the specified variables.
        """
        table_name = kwargs.get('df')

        # Compute dropped variables (everything not in keep list)
        self.deleted_variables = [col for col in supply_frames[table_name].columns if col not in self.vars]
        self.target_tables = [table_name]

        supply_frames[table_name].drop(columns=self.deleted_variables)
        supply_frames[table_name].add_event(self)

        return supply_frames

    def test(self, supply_frames: TableCollection, **kwargs) -> bool:
        """
        Test that only the specified variables remain in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            return False

        cols = supply_frames[table_name].columns
        return set(cols) == set(self.vars)

class DistinctTable(TableTransform):
    """
    Transform class for removing duplicate rows from a DataFrame.
    """

    def __init__(self):
        """
        Initialise a DistinctTable transform.
        """
        super().__init__(
            "DistinctTable",
            "Removes duplicate rows from a DataFrame",
            None,
            "DistinctTbl",
            testable_transform=False,
        )

    def error_check(self, supply_frames: TableCollection, **kwargs):
        """
        Ensure the DataFrame exists and has at least one column and one row.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        df = supply_frames[table_name]
        if df.nvars < 1:
            raise ValueError("DataFrame must have at least one column to apply distinct()")
        if df.nrow < 1:
            raise ValueError("DataFrame must have at least one row to apply distinct()")

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Apply distinct to the DataFrame (all columns).
        """
        table_name = kwargs.get("df")
        self.target_tables = [table_name]

        supply_frames[table_name] = supply_frames[table_name].distinct()
        supply_frames[table_name].add_event(self)

        return supply_frames

class RenameTable(TableTransform):
    """
    Transform class for renaming columns in a DataFrame.
    """
    def __init__(self, rename_map: Dict[str, str]):
        """
        Initialise a RenameTable transform.

        Args:
            rename_map (Dict[str, str]): A dictionary mapping old column names to new column names.
        """
        super().__init__(
            "RenameTable",
            "Renames specified columns in a dataframe",
            list(rename_map.keys()),
            "RenmTbl",
            testable_transform=True
        )
        self.rename_map = rename_map
        self.new_names = list(rename_map.values())

    def error_check(self, supply_frames: TableCollection, **kwargs):
        """
        Validate that all columns to rename exist in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        missing_vars = [var for var in self.rename_map if var not in supply_frames[table_name].columns]
        if missing_vars:
            raise ValueError(f"Columns to rename not found in DataFrame: {missing_vars}")

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Rename the specified columns in the DataFrame.
        """
        table_name = kwargs.get('df')
        self.target_tables = [table_name]

        supply_frames[table_name].rename(columns=self.rename_map, inplace=True)
        supply_frames[table_name].add_event(self)

        return supply_frames

    def test(self, supply_frames: TableCollection, **kwargs) -> bool:
        """
        Test that the columns have been renamed correctly.
        """
        table_name = kwargs.get('df')
        if not table_name:
            return False

        cols = supply_frames[table_name].columns
        return all(new_name in cols for new_name in self.new_names)

class FilterTransform(TableTransform):
    """
    Transform class for filtering rows in a DataFrame using a backend-specific condition.
    """
    def __init__(self, condition_map: Dict[str, Callable]):
        """
        Initialise a FilterTransform.

        Args:
            condition_map (Dict[str, Callable]): 
                A dictionary mapping backend names (e.g., 'pandas', 'polars', 'spark') 
                to filtering functions that accept a DataFrame and return a filtered DataFrame.
        """
        super().__init__(
            "FilterTransform",
            "Filters rows in a dataframe using a backend-specific condition",
            condition_map,
            "RowFilter",
            testable_transform=False
        )

        self.condition_map = condition_map

    def error_check(self, supply_frames: TableCollection, **kwargs):
        """
        Skip error checking — assume condition is valid.
        """
        return True

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Apply the filter condition to the DataFrame.
        """
        table_name = kwargs.get('df')
        self.backend = supply_frames[table_name].frame_type

        lmda = self.condition_map[self.backend]
        self.condition_string = _get_lambda_source(lmda)

        filtered_df = self.vars[self.backend](supply_frames[table_name])
        supply_frames[table_name] = filtered_df
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]

        return supply_frames


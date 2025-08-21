from typing import List, Union

from transforms.base import TableTransform
from tables.collections.collection import TableCollection

from pyspark.sql.functions import col

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
        self.deleted_variables = self.vars
        self.target_tables = [table_name]

        supply_frames[table_name].df = supply_frames[table_name].drop(columns=self.vars)
        supply_frames[table_name].add_event(self)
        return supply_frames

    def test(self, supply_frames: TableCollection, **kwargs) -> bool:
        """
        Test that the variables were successfully removed from the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            return False

        return all(var not in supply_frames[table_name].columns for var in self.vars)

class FilterTransform:
    def __init__(self, condition_map: dict):
        """
        Initialise with a dictionary of backend-specific filter functions.

        Args:
            condition_map (dict): A dictionary with keys 'pandas', 'polars', 'spark',
                                  and values as callables that take a DataFrame and return a filtered one.
        """
        self.condition_map = condition_map
        self.target_tables = []

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Apply the appropriate filter based on the DataFrame backend.

        Args:
            supply_frames (TableCollection): The supply frames collection.
            **kwargs:
                - df (str): The name of the table to apply the filter to.

        Returns:
            MetaFrame: The MetaFrame with filtered rows.
        """
        table_name = kwargs.get('df')
        if table_name not in supply_frames:
            raise ValueError(f"Table '{table_name}' not found in supply_frames.")

        df = supply_frames[table_name].df
        backend = self._detect_backend(df)

        if backend not in self.condition_map:
            raise ValueError(f"No filter condition provided for backend '{backend}'.")

        condition = self.condition_map[backend]

        try:
            df_filtered = condition(df)
        except Exception as e:
            raise RuntimeError(f"Failed to apply filter for backend '{backend}': {e}")

        # Update the table and tracking
        supply_frames[table_name].df = df_filtered
        self.target_tables = [table_name]
        supply_frames[table_name].events.append(self)

        return supply_frames

filter_transform = FilterTransform(condition_map={
    "pandas": lambda df: df[df["age"] > 30],
    "polars": lambda df: df.filter(df["age"] > 30),
    "spark": lambda df: df.filter(col("age") > 30)
})

#result = filter_transform.transforms(supply_frames, df="users_table")

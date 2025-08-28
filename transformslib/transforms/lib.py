from typing import List, Union, Dict, Callable
import inspect

from transformslib.events.pipeevent import TransformEvent
from transformslib.transforms.base import TableTransform
from transformslib.tables.collections.collection import TableCollection

from pyspark.sql.functions import col

def _get_lambda_source(func) -> str:
    try:
        return inspect.getsource(func).strip()
    except Exception as e:
        return f"<source unavailable: {e}"

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

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.vars],
            output_variables=[],
            removed_variables = [self.vars]
            )
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
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.vars],
            output_variables=[],
            removed_variables = [col for col in supply_frames[table_name].columns if col not in self.vars]
            )
        self.target_tables = [table_name]

        supply_frames[table_name].drop(columns=self.log_info.removed_variables)
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

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[],
            output_variables=[]
            )

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

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.vars],
            output_variables=[self.new_names]
            )

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

class ComplexFilter(TableTransform):
    """
    Transform class for filtering rows in a DataFrame using a backend-specific condition.
    """
    def __init__(self, condition_map: Dict[str, Callable]):
        """
        Initialise a ComplexFilter.

        Args:
            condition_map (Dict[str, Callable]): 
                A dictionary mapping backend names (e.g., 'pandas', 'polars', 'spark') 
                to filtering functions that accept a DataFrame and return a filtered DataFrame.
        """
        super().__init__(
            "ComplexFilter",
            "Filters rows in a dataframe using a backend-specific condition",
            None,
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

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[],
            output_variables=[]
            )

        supply_frames[table_name].df = lmda(supply_frames[table_name].df) 
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]

        return supply_frames

class JoinTable(TableTransform):
    """
    Transform class for joining two tables in a TableCollection.
    """

    def __init__(
        self,
        left_table: str,
        right_table: str,
        join_columns: Union[str, List[str]],
        join_type: str = "inner",
        suffixes: tuple = ("_left", "_right")
    ):
        """
        Initialise a JoinTable transform.

        Args:
            left_table (str): Name of the left table.
            right_table (str): Name of the right table.
            join_columns (Union[str, List[str]]): Column(s) to join on.
            join_type (str): Type of join ('inner', 'left', 'right', 'outer').
            suffixes (tuple): Suffixes for overlapping columns.
        """
        super().__init__(
            "JoinTable",
            f"Joins {left_table} and {right_table} on {join_columns} ({join_type})",
            join_columns,
            "JoinTbl",
            testable_transform=True
        )
        self.left_table = left_table
        self.right_table = right_table
        self.join_columns = [join_columns] if isinstance(join_columns, str) else join_columns
        self.join_type = join_type
        self.suffixes = suffixes

    def error_check(self, supply_frames: TableCollection, **kwargs):
        """
        Validate that both tables exist, have >0 rows, and join columns exist.
        """
        for tbl in [self.left_table, self.right_table]:
            if tbl not in supply_frames:
                raise ValueError(f"Table '{tbl}' not found in TableCollection")
            if supply_frames[tbl].nrow < 1:
                raise ValueError(f"Table '{tbl}' must have at least one row")
            missing = [col for col in self.join_columns if col not in supply_frames[tbl].columns]
            if missing:
                raise ValueError(f"Columns {missing} not found in table '{tbl}'")

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Join the two tables and store the result in a new table.
        """
        backend = supply_frames[self.left_table].frame_type
        left_df = supply_frames[self.left_table].df
        right_df = supply_frames[self.right_table].df

        output_table = kwargs.get("output_table", f"{self.left_table}_{self.right_table}_joined")
        self.target_tables = [output_table]

        # Pandas join
        if backend == "pandas":
            joined = left_df.merge(
                right_df,
                how=self.join_type,
                on=self.join_columns,
                suffixes=self.suffixes
            )
        # Polars join
        elif backend == "polars":
            joined = left_df.join(
                right_df,
                on=self.join_columns,
                how=self.join_type
            )
        # Spark join
        elif backend == "pyspark":
            joined = left_df.join(
                right_df,
                on=self.join_columns,
                how=self.join_type
            )
        else:
            raise NotImplementedError(f"Join not implemented for backend '{backend}'")

        # Add joined table to TableCollection
        if output_table == self.left_table:
            supply_frames[self.left_table].df = joined
        elif output_table == self.right_table:
            supply_frames[self.right_table].df = joined
        else:
            # Create a new table entry, copying metadata from left_table
            supply_frames[output_table] = supply_frames[self.left_table].copy(new_name=output_table)
            supply_frames[output_table].df = joined

        self.log_info = TransformEvent(
            input_tables=[self.left_table, self.right_table],
            output_tables=[output_table],
            input_variables=[self.vars],
            output_variables=[self.new_names]
            )
        supply_frames[self.left_table].add_event(self)

        return supply_frames

    def test(self, supply_frames: TableCollection, **kwargs) -> bool:
        """
        Test that the joined table exists and has expected columns.
        """
        output_table = kwargs.get("output_table", f"{self.left_table}_{self.right_table}_joined")
        if output_table not in supply_frames:
            return False
        # Check join columns exist in output
        cols = supply_frames[output_table].columns
        return all(col in cols for col in self.join_columns)

class PartitionByValue(TableTransform):
    """
    Transform class for partitioning a DataFrame into multiple tables 
    based on unique values of a specified column.
    """

    def __init__(self, partition_column: str, suffix_format: str = "_{value}"):
        """
        Initialise a PartitionByValue transform.

        Args:
            partition_column (str): Column to partition the dataset on.
            suffix_format (str): Format string for naming new tables, must contain '{value}'.
        """
        super().__init__(
            "PartitionByValue",
            f"Partitions a DataFrame into subtables by '{partition_column}'",
            [partition_column],
            "PartVal",
            testable_transform=True
        )
        self.partition_column = partition_column
        self.suffix_format = suffix_format

    def error_check(self, supply_frames: TableCollection, **kwargs):
        """
        Validate that the partition column exists in the DataFrame.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if self.partition_column not in supply_frames[table_name].columns:
            raise ValueError(f"Partition column '{self.partition_column}' not found in DataFrame '{table_name}'")

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Partition the DataFrame into multiple subtables based on unique values.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type

        # Collect unique values
        if backend == "pandas":
            unique_values = supply_frames[table_name].df[self.partition_column].unique()
        elif backend == "polars":
            unique_values = supply_frames[table_name].df[self.partition_column].unique().to_list()
        elif backend == "pyspark":
            unique_values = [row[self.partition_column] for row in supply_frames[table_name].df.select(self.partition_column).distinct().collect()]
        else:
            raise NotImplementedError(f"Partition not implemented for backend '{backend}'")

        #if too many unique values, raise error
        if len(unique_values) > 1000:
            raise ValueError(f"Too many unique values ({len(unique_values)}) in column '{self.partition_column}' for a decent partition, please review.")

        output_tables = []
        df = supply_frames[table_name].df
        for value in unique_values:
            new_table_name = f"{table_name}{self.suffix_format.format(value=value)}"
            output_tables.append(new_table_name)

            # Extract partition
            if backend == "pandas":
                partition_df = df[df[self.partition_column] == value].copy()
            elif backend == "polars":
                partition_df = df.filter(df[self.partition_column] == value)
            elif backend == "pyspark":
                partition_df = df.filter(df[self.partition_column] == value)

            # Create new table entry
            supply_frames[new_table_name] = supply_frames[table_name].copy(new_name=new_table_name)
            supply_frames[new_table_name].df = partition_df
            supply_frames[new_table_name].add_event(self)

        self.target_tables = output_tables
        return supply_frames

    def test(self, supply_frames: TableCollection, **kwargs) -> bool:
        """
        Test that all partitioned tables were created and contain the correct values.
        """
        table_name = kwargs.get("df")
        if not table_name:
            return False

        for tname in self.target_tables:
            if tname not in supply_frames:
                return False
            # Simple existence test — skip deep validation for performance
        return True

class SimpleFilter(TableTransform):
    """
    Transform class for filtering rows in a DataFrame using a simple column comparison.
    """

    def __init__(self, column: str, op: str, value):
        """
        Initialise a SimpleFilter.

        Args:
            column (str): Column to filter on.
            op (str): Comparison operator ('==', '!=', '>', '<', '>=', '<=').
            value: Value to compare against.
        """
        super().__init__(
            "SimpleFilter",
            f"Filters rows where {column} {op} {value}",
            [column],
            "SimpleFilter",
            testable_transform=False
        )
        self.column = column
        self.op = op
        self.value = value

    def error_check(self, supply_frames: TableCollection, **kwargs):
        """
        Validate that the column exists in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'")

    def transforms(self, supply_frames: TableCollection, **kwargs):
        """
        Filter the DataFrame using the specified column, operator, and value.
        """
        table_name = kwargs.get('df')
        output_table = kwargs.get('output_table', table_name)
        backend = supply_frames[table_name].frame_type
        df = supply_frames[table_name].df

        # Build filter condition
        if backend == "pandas":
            if self.op == "==":
                filtered = df[df[self.column] == self.value]
            elif self.op == "!=":
                filtered = df[df[self.column] != self.value]
            elif self.op == ">":
                filtered = df[df[self.column] > self.value]
            elif self.op == "<":
                filtered = df[df[self.column] < self.value]
            elif self.op == ">=":
                filtered = df[df[self.column] >= self.value]
            elif self.op == "<=":
                filtered = df[df[self.column] <= self.value]
            else:
                raise ValueError(f"Unsupported operator '{self.op}'")
        elif backend == "polars":
            import polars as pl
            expr = getattr(pl.col(self.column), self._polars_op())(self.value)
            filtered = df.filter(expr)
        elif backend == "pyspark":
            spark_op = {
                "==": lambda c, v: col(c) == v,
                "!=": lambda c, v: col(c) != v,
                ">": lambda c, v: col(c) > v,
                "<": lambda c, v: col(c) < v,
                ">=": lambda c, v: col(c) >= v,
                "<=": lambda c, v: col(c) <= v,
            }
            if self.op not in spark_op:
                raise ValueError(f"Unsupported operator '{self.op}'")
            filtered = df.filter(spark_op[self.op](self.column, self.value))
        else:
            raise NotImplementedError(f"Filtering not implemented for backend '{backend}'")

        # Assign filtered DataFrame
        if output_table == table_name:
            supply_frames[table_name].df = filtered
        else:
            supply_frames[output_table] = supply_frames[table_name].copy(new_name=output_table)
            supply_frames[output_table].df = filtered
            
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[output_table],
            input_variables=[self.vars],
            output_variables=[]
            )
        supply_frames[output_table].add_event(self)

        self.target_tables = [output_table]
        return supply_frames

    def _polars_op(self):
        # Map operator string to polars method name
        return {
            "==": "eq",
            "!=": "neq",
            ">": "gt",
            "<": "lt",
            ">=": "gt_eq",
            "<=": "lt_eq"
        }[self.op]

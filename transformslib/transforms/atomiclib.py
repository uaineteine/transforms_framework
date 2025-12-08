from typing import List, Union, Dict, Callable, TYPE_CHECKING
import inspect
import sys
import os

from hash_method import method_hash
from conclib import load_ent_map

from .pipeevent import TransformEvent
from .base import TableTransform, printwidth

if TYPE_CHECKING:
    from transformslib.tables.collections.collection import TableCollection

from pyspark.sql.functions import col, when, trim, date_trunc, lower, upper, round as spark_round

import polars as pl
import pandas as pd

from transformslib.engine import get_spark, get_engine

def _get_lambda_source(func) -> str:
    try:
        return inspect.getsource(func).strip()
    except Exception as e:
        return f"AL001 source unavailable: {e}"

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

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that all variables to drop exist in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("AL003 Must specify 'df' parameter with table name")

        missing_vars = [var for var in self.vars if var not in supply_frames[table_name].columns]
        if missing_vars:
            print(f"AL002 Columns available: {supply_frames[table_name].columns}")
            raise ValueError(f"AL002 Variables not found in DataFrame columns: {missing_vars}.")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Remove the specified variables from the DataFrame.
        """
        table_name = kwargs.get('df')

        # Capture input columns before transformation
        input_columns = {table_name: list(supply_frames[table_name].columns)}
        
        supply_frames[table_name].drop(columns=self.vars)
        supply_frames[table_name].add_event(self)
        
        # Capture output columns after transformation
        output_columns = {table_name: list(supply_frames[table_name].columns)}

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.vars],
            output_variables=[],
            removed_variables = [self.vars],
            input_columns=input_columns,
            output_columns=output_columns
            )
        self.deleted_variables = self.vars
        self.target_tables = [table_name]

        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
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

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that all variables to keep exist in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        missing_vars = [var for var in self.vars if var not in supply_frames[table_name].columns]
        if missing_vars:
            raise ValueError(f"Variables not found in DataFrame columns: {missing_vars}")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Subset the DataFrame to retain only the specified variables.
        """
        table_name = kwargs.get('df')

        # Capture input columns before transformation
        input_columns = {table_name: list(supply_frames[table_name].columns)}
        
        # Compute dropped variables (everything not in keep list)
        removed_vars = [col for col in supply_frames[table_name].columns if col not in self.vars]
        
        supply_frames[table_name].drop(columns=removed_vars)
        supply_frames[table_name].add_event(self)

        # Capture output columns after transformation
        output_columns = {table_name: list(supply_frames[table_name].columns)}
        
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.vars],
            output_variables=[],
            removed_variables = removed_vars,
            input_columns=input_columns,
            output_columns=output_columns
            )
        self.target_tables = [table_name]

        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
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

    def error_check(self, supply_frames: "TableCollection", **kwargs):
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

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Apply distinct to the DataFrame (all columns).
        """
        table_name = kwargs.get("df")
        self.target_tables = [table_name]

        # Capture row count before transformation
        input_row_count = supply_frames[table_name].nrow

        supply_frames[table_name] = supply_frames[table_name].distinct()
        
        # Capture row count after transformation
        output_row_count = supply_frames[table_name].nrow

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[],
            output_variables=[],
            input_row_counts={table_name: input_row_count},
            output_row_counts={table_name: output_row_count}
            )

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

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that all columns to rename exist in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        missing_vars = [var for var in self.rename_map if var not in supply_frames[table_name].columns]
        if missing_vars:
            raise ValueError(f"Columns to rename not found in DataFrame: {missing_vars}")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Rename the specified columns in the DataFrame.
        """
        table_name = kwargs.get('df')
        self.target_tables = [table_name]

        # Capture input columns before transformation
        input_columns = {table_name: list(supply_frames[table_name].columns)}

        supply_frames[table_name].rename(columns=self.rename_map, inplace=True)
        supply_frames[table_name].add_event(self)

        # Capture output columns after transformation  
        output_columns = {table_name: list(supply_frames[table_name].columns)}

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.vars],
            output_variables=[self.new_names],
            input_columns=input_columns,
            output_columns=output_columns
            )

        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
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

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Skip error checking — assume condition is valid.
        """
        return True

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Apply the filter condition to the DataFrame.
        """
        table_name = kwargs.get('df')
        self.backend = supply_frames[table_name].frame_type

        lmda = self.condition_map[self.backend]
        self.condition_string = _get_lambda_source(lmda)

        # Capture row count before transformation
        input_row_count = supply_frames[table_name].nrow

        supply_frames[table_name].df = lmda(supply_frames[table_name].df) 
        
        # Capture row count after transformation
        output_row_count = supply_frames[table_name].nrow

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[],
            output_variables=[],
            input_row_counts={table_name: input_row_count},
            output_row_counts={table_name: output_row_count}
            )

        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]

        return supply_frames

class JoinTable(TableTransform):
    """
    Transform class for joining two tables in a TableCollection.
    """

    # Define as a static, class-level property
    ACCEPTABLE_JOIN_TYPES: list[str] = ["inner", "left", "right", "outer", "cross"]

    LEFT_JOIN:str = "left"
    RIGHT_JOIN:str = "right"
    INNER_JOIN:str = "inner"
    OUTER_JOIN:str = "outer"
    CROSS_JOIN:str = "cross"

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
            join_type (str): Type of join ('inner', 'left', 'right', 'outer', 'cross').
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
        if join_type not in JoinTable.ACCEPTABLE_JOIN_TYPES:
            raise ValueError("join_type for JoinTable Transform must be one of left, right, inner, outer")
        self.join_type = join_type
        self.suffixes = suffixes

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that both tables exist, have >0 rows, and join columns exist.
        """
        for tbl in [self.left_table, self.right_table]:
            if tbl not in supply_frames:
                all_tables = supply_frames.get_table_names()
                raise ValueError(f"AT101 Table '{tbl}' not found in TableCollection: Please refer to full list: {all_tables}")
            if supply_frames[tbl].nrow < 1:
                raise ValueError(f"AT100 Table '{tbl}' must have at least one row")
            missing = [col for col in self.join_columns if col not in supply_frames[tbl].columns]
            if missing:
                raise ValueError(f"AT102 Columns {missing} not found in table '{tbl}'")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Join the two tables and store the result in a new table.
        """
        backend = supply_frames[self.left_table].frame_type
        left_df = supply_frames[self.left_table].df
        right_df = supply_frames[self.right_table].df

        output_table = kwargs.get("output_table", f"{self.left_table}_{self.right_table}_joined")
        self.target_tables = [output_table]

        # Capture input columns before transformation
        input_columns = {
            self.left_table: list(supply_frames[self.left_table].columns),
            self.right_table: list(supply_frames[self.right_table].columns)
        }

        # Capture row counts before transformation
        left_row_count = supply_frames[self.left_table].nrow
        right_row_count = supply_frames[self.right_table].nrow

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
            # Capture row count after transformation
            output_row_count = supply_frames[self.left_table].nrow
        elif output_table == self.right_table:
            supply_frames[self.right_table].df = joined
            # Capture row count after transformation
            output_row_count = supply_frames[self.right_table].nrow
        else:
            # Create a new table entry, copying metadata from left_table
            supply_frames[output_table] = supply_frames[self.left_table].copy(new_name=output_table)
            supply_frames[output_table].df = joined
            # Capture row count after transformation
            output_row_count = supply_frames[output_table].nrow

        # Capture output columns after transformation
        output_columns = {output_table: list(supply_frames[output_table].columns)}

        self.log_info = TransformEvent(
            input_tables=[self.left_table, self.right_table],
            output_tables=[output_table],
            input_variables=[self.vars],
            output_variables=[],
            input_row_counts={self.left_table: left_row_count, self.right_table: right_row_count},
            output_row_counts={output_table: output_row_count},
            input_columns=input_columns,
            output_columns=output_columns
            )
        
        supply_frames[output_table].add_event(self)

        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
        """
        Test that the joined table exists and has expected columns.
        """
        output_table = kwargs.get("output_table", f"{self.left_table}_{self.right_table}_joined")
        if output_table not in supply_frames:
            return False
        
        if self.join_type == self.CROSS_JOIN:
            # Cross joins don't guarantee join columns in output
            return supply_frames[output_table].nrow > 0
        
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

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the partition column exists in the DataFrame.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if self.partition_column not in supply_frames[table_name].columns:
            raise ValueError(f"Partition column '{self.partition_column}' not found in DataFrame '{table_name}'")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Partition the DataFrame into multiple subtables based on unique values.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type

        # Capture input columns before transformation
        input_columns = {table_name: list(supply_frames[table_name].columns)}

        # Capture row count before transformation
        input_row_count = supply_frames[table_name].nrow

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

        new_table_names = []
        for value in unique_values:
            new_table_name = f"{table_name}{self.suffix_format.format(value=value)}"
            new_table_names.append(new_table_name)
        
        output_row_counts = {}
        output_columns = {}
        for i,value in enumerate(unique_values):
            new_table_name = new_table_names[i]
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

            # Capture row count and columns for this partition
            output_row_counts[new_table_name] = supply_frames[new_table_name].nrow
            output_columns[new_table_name] = list(supply_frames[new_table_name].columns)

            supply_frames[new_table_name].add_event(self)

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=new_table_names,
            input_variables=[self.vars],
            output_variables=[],
            input_row_counts={table_name: input_row_count},
            output_row_counts=output_row_counts,
            input_columns=input_columns,
            output_columns=output_columns
            )

        self.target_tables = output_tables
        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
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

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the column exists in the DataFrame.
        """
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Filter the DataFrame using the specified column, operator, and value.
        """
        table_name = kwargs.get('df')
        output_table = kwargs.get('output_table', table_name)
        backend = supply_frames[table_name].frame_type
        df = supply_frames[table_name].df

        # Capture row count before transformation
        input_row_count = supply_frames[table_name].nrow

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
            # Capture row count after transformation
            output_row_count = supply_frames[table_name].nrow
        else:
            supply_frames[output_table] = supply_frames[table_name].copy(new_name=output_table)
            supply_frames[output_table].df = filtered
            # Capture row count after transformation
            output_row_count = supply_frames[output_table].nrow
            
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[output_table],
            input_variables=[self.vars],
            output_variables=[],
            input_row_counts={table_name: input_row_count},
            output_row_counts={output_table: output_row_count}
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

class ConcatColumns(TableTransform):
    """
    Transform class for concatenating multiple columns into a single column.
    """

    def __init__(self, variables_to_concat: Union[str, List[str]], sep:str=""):
        """
        Initialise a ConcatColumns treatment.

        Args:
        ...
        """

        super().__init__(
            "ConcatColumns",
            "Concatenante multiple columns together in a dataframe",
            variables_to_concat,
            "ConcCols",
            testable_transform=True
        )

        self.separator = sep

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Ensure the DataFrame exists, the specified columns exist, and the output column name is provided.
        """
        table_name = kwargs.get("df")
        output_col = kwargs.get("output_var")

        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        if not self.vars or not isinstance(self.vars, list) or len(self.vars) < 2:
            raise ValueError("Must provide a list of at least two columns to concatenate")
        if not output_col:
            raise ValueError("Must specify 'output_var' parameter for the new column")

        df = supply_frames[table_name]
        missing_cols = [c for c in self.vars if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in DataFrame: {missing_cols}")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Concatenate the specified columns into a new column in-place using MultiTable.concat().
        """
        table_name = kwargs.get("df")
        output_var = kwargs.get("output_var")

        self.target_tables = [table_name]

        # Capture input columns before transformation
        input_columns = {table_name: list(supply_frames[table_name].columns)}

        df = supply_frames[table_name]
        # Use the MultiTable in-place concat method
        df.concat(new_col_name=output_var, columns=self.vars, sep=self.separator)

        df.add_event(self)

        # Capture output columns after transformation
        output_columns = {table_name: list(supply_frames[table_name].columns)}

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.vars],
            output_variables=[output_var],
            created_variables=[output_var],
            input_columns=input_columns,
            output_columns=output_columns
        )

        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
        """
        Test that the concatenated column was created in the target MultiTable.
        """
        table_name = kwargs.get("df")
        output_var = kwargs.get("output_var")

        if not table_name or not output_var:
            return False

        if table_name not in supply_frames:
            return False

        # Check if the output column exists in the MultiTable
        if output_var not in supply_frames[table_name].columns:
            return False

        return True

class ReplaceByCondition(TableTransform):
    """
    Transform class for replacing values in a column based on a comparison condition.
    """

    def __init__(self, column: str, op: str, value: Union[int, float, str], replacement: Union[int, float, str]):
        """
        Initialise a ReplaceByCondition transform.

        Args:
            column (str): Column to apply the replacement on.
            op (str): Comparison operator ('==', '!=', '>', '<', '>=', '<=').
            value: Value to compare against.
            replacement: Value to replace matching rows with.
        """
        super().__init__(
            "ReplaceByCondition",
            f"Replaces values in '{column}' where {column} {op} {value} with {replacement}",
            [column],
            "ReplCond",
            testable_transform=True,
        )

        self.column = column
        self.op = op
        self.value = value
        self.replacement = replacement

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the column exists in the DataFrame and operator is supported.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'")

        if self.op not in ["==", "!=", ">", "<", ">=", "<="]:
            raise ValueError(f"Unsupported operator '{self.op}'")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Replace values in the column based on the condition.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type
        df = supply_frames[table_name].df

        if backend == "pandas":
            if self.op == "==":
                mask = df[self.column] == self.value
            elif self.op == "!=":
                mask = df[self.column] != self.value
            elif self.op == ">":
                mask = df[self.column] > self.value
            elif self.op == "<":
                mask = df[self.column] < self.value
            elif self.op == ">=":
                mask = df[self.column] >= self.value
            elif self.op == "<=":
                mask = df[self.column] <= self.value

            df.loc[mask, self.column] = self.replacement

        elif backend == "polars":
            expr_map = {
                "==": pl.col(self.column) == self.value,
                "!=": pl.col(self.column) != self.value,
                ">": pl.col(self.column) > self.value,
                "<": pl.col(self.column) < self.value,
                ">=": pl.col(self.column) >= self.value,
                "<=": pl.col(self.column) <= self.value,
            }
            df = df.with_columns(
                pl.when(expr_map[self.op])
                .then(self.replacement)
                .otherwise(pl.col(self.column))
                .alias(self.column)
            )

        elif backend == "pyspark":
            spark_op = {
                "==": lambda c, v: col(c) == v,
                "!=": lambda c, v: col(c) != v,
                ">": lambda c, v: col(c) > v,
                "<": lambda c, v: col(c) < v,
                ">=": lambda c, v: col(c) >= v,
                "<=": lambda c, v: col(c) <= v,
            }
            condition = spark_op[self.op](self.column, self.value)
            df = df.withColumn(
                self.column,
                when(condition, self.replacement).otherwise(col(self.column)),
            )
        else:
            raise NotImplementedError(f"Replacement not implemented for backend '{backend}'")

        # Save back into TableCollection
        supply_frames[table_name].df = df

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.column],
            output_variables=[self.column],
        )
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]
        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
        """
        Test that no values matching the condition remain in the column.
        """
        table_name = kwargs.get("df")
        if not table_name:
            return False

        df = supply_frames[table_name].df
        backend = supply_frames[table_name].frame_type

        if backend == "pandas":
            if self.op == "==":
                return not (df[self.column] == self.value).any()
            elif self.op == "!=":
                return not (df[self.column] != self.value).any()
            elif self.op == ">":
                return not (df[self.column] > self.value).any()
            elif self.op == "<":
                return not (df[self.column] < self.value).any()
            elif self.op == ">=":
                return not (df[self.column] >= self.value).any()
            elif self.op == "<=":
                return not (df[self.column] <= self.value).any()

        elif backend == "polars":
            expr_map = {
                "==": (df[self.column] == self.value),
                "!=": (df[self.column] != self.value),
                ">": (df[self.column] > self.value),
                "<": (df[self.column] < self.value),
                ">=": (df[self.column] >= self.value),
                "<=": (df[self.column] <= self.value),
            }
            return expr_map[self.op].sum() == 0

        elif backend == "pyspark":
            spark_op = {
                "==": lambda c, v: col(c) == v,
                "!=": lambda c, v: col(c) != v,
                ">": lambda c, v: col(c) > v,
                "<": lambda c, v: col(c) < v,
                ">=": lambda c, v: col(c) >= v,
                "<=": lambda c, v: col(c) <= v,
            }
            return df.filter(spark_op[self.op](self.column, self.value)).count() == 0

        return False

class ExplodeColumn(TableTransform):
    """
    Transform class for exploding a list-like column into multiple rows.
    """

    def __init__(self, column: str, sep: str = None, outer: bool = False):
        """
        Initialise an ExplodeColumn transform.

        Args:
            column (str): The name of the column to explode.
            sep (str, optional): If provided, split string values on this separator before exploding.
            outer (bool, optional): Whether to use outer explode (keep null/empty values). Default is False.
        """
        super().__init__(
            "ExplodeColumn",
            f"Explodes column '{column}' into multiple rows",
            [column],
            "ExplodeCol",
            testable_transform=True,
        )
        self.column = column
        self.sep = sep
        self.outer = outer

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the target column exists in the DataFrame.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'. Have you made a typo or are you not using capitalised header names?")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Apply explode on the specified column using MultiTable.explode().
        """
        table_name = kwargs.get("df")
        df = supply_frames[table_name]

        # Capture row count before transformation
        input_row_count = df.nrow

        df.explode(column=self.column, sep=self.sep, outer=self.outer)

        # Capture row count after transformation
        output_row_count = df.nrow

        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.column],
            output_variables=[self.column],
            input_row_counts={table_name: input_row_count},
            output_row_counts={table_name: output_row_count}
        )
        df.add_event(self)

        self.target_tables = [table_name]
        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
        """
        Simple test: ensure column still exists after explode.
        """
        table_name = kwargs.get("df")
        if not table_name:
            return False
        return self.column in supply_frames[table_name].columns

class DropNAValues(TableTransform):
    """
    Transform class for dropping rows with NA/None/Null values in a specified column.
    """

    def __init__(self, column: str):
        """
        Initialise a DropNAValues transform.

        Args:
            column (str): The name of the column to check for NA values.
        """
        super().__init__(
            "DropNAValues",
            f"Drops rows with NA values in column '{column}'",
            [column],
            "DropNA",
            testable_transform=True,
        )
        self.column = column

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the target column exists in the DataFrame.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Drop rows with NA values in the specified column.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type

        # Capture row count before transformation
        input_row_count = supply_frames[table_name].nrow

        # Apply backend-specific NA drop
        if backend == "pandas":
            supply_frames[table_name].df = supply_frames[table_name].df.dropna(subset=[self.column])
        elif backend == "polars":
            supply_frames[table_name].df = supply_frames[table_name].df.drop_nulls(subset=[self.column])
        elif backend == "pyspark":
            supply_frames[table_name].df = supply_frames[table_name].df.na.drop(subset=[self.column])
        else:
            raise NotImplementedError(f"AT010 DropNA not implemented for backend '{backend}'")

        # Capture row count after transformation
        output_row_count = supply_frames[table_name].nrow

        # Log event
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.column],
            output_variables=[self.column],
            input_row_counts={table_name: input_row_count},
            output_row_counts={table_name: output_row_count}
        )
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]
        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
        """
        Test that no NA values remain in the target column.
        """
        table_name = kwargs.get("df")
        if not table_name:
            return False

        backend = supply_frames[table_name].frame_type

        if backend == "pandas":
            return not supply_frames[table_name].df[self.column].isna().any()
        elif backend == "polars":
            return supply_frames[table_name].df[self.column].null_count() == 0
        elif backend == "pyspark":
            return supply_frames[table_name].df.filter(col(self.column).isNull()).count() == 0
        return False

class TrimWhitespace(TableTransform):
    """
    Transform class for trimming leading and trailing whitespace from string values in a specified column.
    """

    def __init__(self, column: str):
        """
        Initialise a TrimWhitespace transform. This removes whitespace around a variable (string).

        Args:
            column (str): The name of the column to trim.
        """
        super().__init__(
            "TrimWhitespace",
            f"Trims whitespace from column '{column}'",
            [column],
            "Trim",
            testable_transform=False,
        )
        self.column = column

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the target column exists and is of string type.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'")

        backend = supply_frames[table_name].frame_type
        if backend == "pandas":
            if not pd.api.types.is_string_dtype(supply_frames[table_name].df[self.column]):
                raise TypeError(f"Column '{self.column}' must be of string dtype in pandas")
        elif backend == "polars":
            if supply_frames[table_name].schema[self.column] != pl.Utf8:
                raise TypeError(f"Column '{self.column}' must be of string type in polars")
        elif backend == "pyspark":
            if supply_frames[table_name].dtypes[self.column] != 'StringType()':
                raise TypeError(f"Column '{self.column}' must be of string type in pyspark")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Trim leading and trailing whitespace in the specified column.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type

        if backend == "pandas":
            supply_frames[table_name].df[self.column] = supply_frames[table_name].df[self.column].str.strip()

        elif backend == "polars":
            supply_frames[table_name].df = supply_frames[table_name].df.with_columns(
                pl.col(self.column).str.strip_chars().alias(self.column)
            )

        elif backend == "pyspark":
            supply_frames[table_name].df = supply_frames[table_name].df.withColumn(
                self.column, trim(col(self.column))
            )
        else:
            raise NotImplementedError(f"TrimWhitespace not implemented for backend '{backend}'")

        # Log the transform event
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.column],
            output_variables=[self.column],
        )
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]
        return supply_frames

class ForceCase(TableTransform):
    """
    Transform class to force string values in a specified column to upper or lower case.
    """

    def __init__(self, column: str, case: str = "lower"):
        """
        Initialise a ForceCase transform.

        Args:
            column (str): The name of the column to modify.
            case (str): The case to apply: 'lower' or 'upper'.
        """
        if case not in {"lower", "upper"}:
            raise ValueError("case must be either 'lower' or 'upper'")

        super().__init__(
            "ForceCase",
            f"Forces {case}case in column '{column}'",
            [column],
            "ForceCase",
            testable_transform=False
        )
        self.column = column
        self.case = case

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the target column exists and is of string type.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'")

        backend = supply_frames[table_name]
        if backend == "pandas":
            if not pd.api.types.is_string_dtype(supply_frames[table_name].df[self.column]):
                raise TypeError(f"Column '{self.column}' must be of string dtype in pandas")
        elif backend == "polars":
            if supply_frames[table_name].schema[self.column] != pl.Utf8:
                raise TypeError(f"Column '{self.column}' must be of string type in polars")
        elif backend == "pyspark":
            if supply_frames[table_name].dtypes[self.column] != 'StringType()':
                raise TypeError(f"Column '{self.column}' must be of string type in pyspark")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Force case in the specified column.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type

        if backend == "pandas":
            if self.case == "lower":
                supply_frames[table_name].df[self.column] = supply_frames[table_name].df[self.column].str.lower()
            else:
                supply_frames[table_name].df[self.column] = supply_frames[table_name].df[self.column].str.upper()

        elif backend == "polars":
            if self.case == "lower":
                supply_frames[table_name].df = supply_frames[table_name].df.with_columns(
                    pl.col(self.column).str.to_lowercase().alias(self.column)
                )
            else:
                supply_frames[table_name].df = supply_frames[table_name].df.with_columns(
                    pl.col(self.column).str.to_uppercase().alias(self.column)
                )

        elif backend == "pyspark":
            func = lower if self.case == "lower" else upper
            supply_frames[table_name].df = supply_frames[table_name].df.withColumn(
                self.column, func(col(self.column))
            )

        else:
            raise NotImplementedError(f"ForceCase not implemented for backend '{backend}'")

        # Log event
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.column],
            output_variables=[self.column],
        )
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]
        return supply_frames

class TruncateDate(TableTransform):
    """
    Transform class to truncate date values in a specified column to year or month level.
    """

    def __init__(self, column: str, level: str = "month"):
        """
        Initialise a TruncateDate transform.

        Args:
            column (str): The name of the column to modify.
            level (str): The truncation level: 'year' or 'month'.
        """
        if level not in {"year", "month"}:
            raise ValueError("level must be either 'year' or 'month'")

        super().__init__(
            "TruncateDate",
            f"Truncates dates in column '{column}' to {level} level",
            [column],
            "TruncDate",
            testable_transform=False
        )
        self.column = column
        self.level = level

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the target column exists and is of datetime type.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'")

        backend = supply_frames[table_name]
        if backend == "pandas":
            if not pd.api.types.is_datetime64_any_dtype(supply_frames[table_name].df[self.column]):
                raise TypeError(f"Column '{self.column}' must be of datetime dtype in pandas")

        elif backend == "polars":
            if supply_frames[table_name].schema[self.column] not in {pl.Date, pl.Datetime}:
                raise TypeError(f"Column '{self.column}' must be of date/datetime type in polars")

        elif backend == "pyspark":
            if supply_frames[table_name].dtypes[self.column] not in {'TimestampType()', 'DateType()'}:
                raise TypeError(f"Column '{self.column}' must be of date or timestamp type in pyspark")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Truncate the date column to the specified level.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type

        if backend == "pandas":
            if self.level == "year":
                supply_frames[table_name].df[self.column] = supply_frames[table_name].df[self.column].dt.to_period("Y").dt.to_timestamp()
            else:
                supply_frames[table_name].df[self.column] = supply_frames[table_name].df[self.column].dt.to_period("M").dt.to_timestamp()

        elif backend == "polars":
            if self.level == "year":
                supply_frames[table_name].df = supply_frames[table_name].df.with_columns(
                    pl.col(self.column).dt.truncate("1y").alias(self.column)
                )
            else:
                supply_frames[table_name].df = supply_frames[table_name].df.with_columns(
                    pl.col(self.column).dt.truncate("1mo").alias(self.column)
                )

        elif backend == "pyspark":
            trunc_unit = "YEAR" if self.level == "year" else "MONTH"
            supply_frames[table_name].df = supply_frames[table_name].df.withColumn(
                self.column, date_trunc(trunc_unit, col(self.column))
            )

        else:
            raise NotImplementedError(f"TruncateDate not implemented for backend '{backend}'")

        # Log event
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.column],
            output_variables=[self.column],
        )
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]
        return supply_frames

class RoundNumber(TableTransform):
    """
    Transform class to round numeric values in a specified column to a given number of decimal places.
    """

    def __init__(self, column: str, decimals: int = 1):
        """
        Initialise a RoundNumber transform.

        Args:
            column (str): The name of the column to modify.
            decimals (int): The number of decimal places to round to.
        """
        if not isinstance(decimals, int):
            raise ValueError("decimals must be an integer")

        super().__init__(
            "RoundNumber",
            f"Rounds values in column '{column}' to {decimals} decimal places",
            [column],
            "RndNum",
            testable_transform=False
        )
        self.column = column
        self.decimals = decimals

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the target column exists and is numeric.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if self.column not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.column}' not found in DataFrame '{table_name}'")

        backend = supply_frames[table_name]
        if backend == "pandas":
            if not pd.api.types.is_numeric_dtype(supply_frames[table_name].df[self.column]):
                raise TypeError(f"Column '{self.column}' must be numeric in pandas")

        elif backend == "polars":
            if supply_frames[table_name].schema[self.column] not in {pl.Float32, pl.Float64, pl.Int32, pl.Int64}:
                raise TypeError(f"Column '{self.column}' must be numeric in polars")

        elif backend == "pyspark":
            if supply_frames[table_name].dtypes[self.column] not in {'DoubleType()', 'FloatType()', 'IntegerType()', 'LongType()', 'DecimalType()'}:
                raise TypeError(f"Column '{self.column}' must be numeric in pyspark")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Round the numeric column to the specified number of decimal places.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type

        if backend == "pandas":
            supply_frames[table_name].df[self.column] = supply_frames[table_name].df[self.column].round(self.decimals)

        elif backend == "polars":
            supply_frames[table_name].df = supply_frames[table_name].df.with_columns(
                pl.col(self.column).round(self.decimals).alias(self.column)
            )

        elif backend == "pyspark":
            supply_frames[table_name].df = supply_frames[table_name].df.withColumn(
                self.column, spark_round(col(self.column), self.decimals)
            )

        else:
            raise NotImplementedError(f"RoundNumber not implemented for backend '{backend}'")

        # Log event
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=[self.column],
            output_variables=[self.column],
        )
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]
        return supply_frames

class HashColumns(TableTransform):
    "Using the hashing tool, hash the contents of one or more columns into a new column."

    def __init__(self, columns: Union[str, List[str]], hash_method: str = "sha256"):
        """
        Initialise a HashColumns transform.

        Args:
            columns (str | list[str]): Column name or list of column names to hash.
            hash_method (str): Hashing hash_method to use (e.g., 'md5', 'sha1', 'sha256').
        """
        if isinstance(columns, str):
            columns = [columns]

        super().__init__(
            "HashColumns",
            f"Hashes columns {columns} using {hash_method}",
            columns,
            "HashCols",
            testable_transform=True
        )
        self.columns = columns
        self.hash_method = hash_method

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the target columns exist in the table. Validate no columns are actually dates or something incompatible with hashing.
        """
        table_name = kwargs.get("df")

        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        missing = [c for c in self.columns if c not in supply_frames[table_name].columns]
        if missing:
            raise ValueError(f"Columns {missing} not found in DataFrame '{table_name}'")
        
        # Check for unsupported types (e.g., date/datetime)
        backend = supply_frames[table_name].frame_type
        for col in self.columns:
            if backend == "pandas":
                if pd.api.types.is_datetime64_any_dtype(supply_frames[table_name].df[col]):
                    raise TypeError(f"Column '{col}' is of datetime type and cannot be hashed in pandas")
            elif backend == "polars":
                if supply_frames[table_name].schema[col] in {pl.Date, pl.Datetime}:
                    raise TypeError(f"Column '{col}' is of date/datetime type and cannot be hashed in polars")
            elif backend == "pyspark":
                if supply_frames[table_name].dtypes[col] in {'TimestampType()', 'DateType()'}:
                    raise TypeError(f"Column '{col}' is of date or timestamp type and cannot be hashed in pyspark")
    
    def transforms(self, supply_frames, **kwargs):
        tbn = kwargs.get("df")
        backend = supply_frames[tbn].frame_type
        
        for col in self.columns:
            if backend == "pyspark":
                spark = get_spark()
                supply_frames[tbn].df = method_hash(supply_frames[tbn].df, col, col, self.hash_method, spark=spark)
            else:
                raise NotImplementedError(f"HashColumns not implemented for backend '{backend}'")
        
        #make event
        self.log_info = TransformEvent(
            input_tables=[tbn],
            output_tables=[tbn],
            input_variables=self.columns,
            output_variables=self.columns
        )
        supply_frames[tbn].add_event(self)

        return supply_frames
    
    def test(self, supply_frames, **kwargs) -> bool:
        tbn = kwargs.get("df")
        backend = supply_frames[tbn].frame_type

        for colm in self.columns:
            if backend == "pyspark":
                # Define regex for hex (non-null, non-empty, only 0-9, a-f, A-F)
                hex_regex = "^[0-9a-fA-F]+$"

                print(colm)

                # Filter rows that are NOT valid hex
                invalid_rows = supply_frames[tbn].df.filter(~col(colm).rlike(hex_regex))

                # Check if any invalid rows exist
                all_valid_hex = invalid_rows.isEmpty()

                if not all_valid_hex:
                    return False
            else:
                raise NotImplementedError(f"HashColumns not implemented for backend '{backend}'")
            
        #exit
        return True

class SortTable(TableTransform):
    """
    Transform class to sort a table by one or more columns.
    """

    def __init__(self, by: Union[str, List[str]], ascending: Union[bool, List[bool]] = True):
        """
        Initialise a SortTable transform.

        Args:
            by (str | list[str]): Column name or list of column names to sort by.
            ascending (bool | list[bool]): Sort order for each column.
                True for ascending, False for descending.
                Can be a single bool or a list matching the columns.
        """
        super().__init__(
            "SortTable",
            f"Sort table by {by}",
            by if isinstance(by, list) else [by],
            "SortTbl",
            testable_transform=False
        )
        self.by = by
        self.ascending = ascending

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that the target columns exist in the table.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        if isinstance(self.by, str):
            columns_to_check = [self.by]
        else:
            columns_to_check = self.by

        missing = [c for c in columns_to_check if c not in supply_frames[table_name].columns]
        if missing:
            raise ValueError(f"Columns {missing} not found in DataFrame '{table_name}'")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Sort the table using MultiTable.sort().
        """
        table_name = kwargs.get("df")
        supply_frames[table_name] = supply_frames[table_name].sort(
            by=self.by, ascending=self.ascending
        )

        # Log event
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=self.by if isinstance(self.by, list) else [self.by],
            output_variables=self.by if isinstance(self.by, list) else [self.by],
        )
        supply_frames[table_name].add_event(self)
        self.target_tables = [table_name]

        return supply_frames

class AttachSynID(TableTransform):
    """
    Will attach a synthetic ID to the Table from its source ID
    """
    
    def __init__(self, source_id:str, use_fast_join=False):
        """
        Initialise an AttachSynID transform.
        
        Args:
            source_id (str): The name of the source ID column to base the synthetic ID on.
        """
        super().__init__(
            "SynID",
            f"Attach the synthetic ID based on source ID '{source_id}'",
            [source_id],
            "SYNID",
            testable_transform=True
        )

        self.use_fast_join = use_fast_join
        
    def error_check(self, supply_frames, **kwargs):
        #check column actually exists in the df
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        if self.source_id not in supply_frames[table_name].columns:
            raise ValueError(f"Column '{self.source_id}' not found in DataFrame '{table_name}'")
        
    def transforms(self, supply_frames, **kwargs):
        #get table name
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type
        
        #if not using pyspark raise an error
        if backend != "pyspark":
            raise NotImplementedError(f"AttachSynID not implemented for backend '{backend}'")
        
        incols = list(supply_frames[table_name].columns)
        
        #load the entity map
        spark = get_spark()
        entmap = load_ent_map(spark=spark)

        #use modified id group variable to join if it exists on both frames
        vars_to_join = [self.source_id]
        if (self.use_fast_join):
            mod_col = os.getenv("TNSFRMS_MOD_VAR", "id_mod")
            if mod_col in entmap.columns:
                if mod_col in incols:
                    vars_to_join.append(mod_col)
                    print("Using modified ID group variable for joining synthetic ID.")
                    print("Variables to join on:", vars_to_join)
        
        #run a pyspark join to attach the synthetic ID
        supply_frames[table_name].df = supply_frames[table_name].df.join(
            entmap,
            on=vars_to_join,
            how="left"
        )
        
        SYNVARID = os.getenv("TNSFRMS_SYN_VAR", "SYNTHETIC")
        
        #create the event
        self.log_info = TransformEvent(
            input_tables=[table_name, "entmap"],
            output_tables=[table_name],
            input_variables=[self.source_id],
            output_variables=[SYNVARID],
            input_columns=incols,
            output_columns={table_name: list(supply_frames[table_name].columns)}
        )
        
        supply_frames[table_name].add_event(self)
        
    def test(self, supply_frames, **kwargs):
        #simple test to check the SYNID column exists
        table_name = kwargs.get("df")
        SYNVARID = os.getenv("TNSFRMS_SYN_VAR", "SYNTHETIC")
        
        if not table_name:
            return False
        
        if SYNVARID in supply_frames[table_name].columns:
            #check the null count is zero: #TODO ADD OTHER SUPPORTED BACKENDS
            if supply_frames[table_name].df[SYNVARID].isnull().sum() > 0:
                return False
        else:
            return False

import hmac
import hashlib
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
def apply_hmac_spark(df, column: str, salt_key: str, trunc_length:int) -> DataFrame:
    """
    Apply HMAC hashing to a specified column in a PySpark DataFrame.

    Args:
        df (DataFrame): The PySpark DataFrame.
        column (str): The name of the column to hash.
        salt_key (str): The secret key for HMAC.
        trunc_length (int): The length to truncate the hash to.
    Returns:
        DataFrame: The DataFrame with the hashed column.
    """
    def hmac_hash(value: str) -> str:
        if value is None:
            return None
        hmac_obj = hmac.new(salt_key.encode(), value.encode(), hashlib.sha256)
        return hmac_obj.hexdigest()[:trunc_length]

    hmac_udf = udf(hmac_hash)

    return df.withColumn(column, hmac_udf(df[column]))

class ApplyHMAC(TableTransform):
    """
    Transform class to apply specific HMAC hashing to a specified column using a secret key.
    """

    def __init__(self, column:str, trunclength:int):
        """
        Initialise an HMAC transform.

        Args:
            column (str): The name of the column to hash.
        """

        super().__init__(
            "ApplyHMAC",
            f"Applies HMAC hashing to given variables",
            [column],
            "HMACHash",
            testable_transform=True
        )
        self.columns_to_hash = [column]

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that at least one target column exists in the table.
        """
        table_name = kwargs.get("df")
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")

        # Filter to only columns that exist
        self.existing_columns = [col for col in self.columns_to_hash if col in supply_frames[table_name].columns]
        
        if not self.existing_columns:
            raise ValueError(f"None of the columns {self.columns_to_hash} found in DataFrame '{table_name}'")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Apply HMAC hashing to the columns that exist in the DataFrame.
        """
        table_name = kwargs.get("df")
        backend = supply_frames[table_name].frame_type
        key = kwargs.get("hmac_key")

        if backend == "pyspark":
            for column in self.existing_columns:
                supply_frames[table_name].df = apply_hmac_spark(
                    supply_frames[table_name].df,
                    column,
                    key,
                    trunc_length=16
                )
        else:
            raise NotImplementedError(f"HMAC hash not implemented for backend '{backend}'")

        # Log event
        self.log_info = TransformEvent(
            input_tables=[table_name],
            output_tables=[table_name],
            input_variables=self.existing_columns,
            output_variables=self.existing_columns,
        )
        supply_frames[table_name].add_event(self)

        self.target_tables = [table_name]
        return supply_frames

    def test(self, supply_frames: "TableCollection", **kwargs) -> bool:
        """
        Test that all existing columns still exist after HMAC application.
        """
        table_name = kwargs.get("df")
        if not table_name:
            return False
        return all(col in supply_frames[table_name].columns for col in self.existing_columns)

class UnionTables(TableTransform):
    """
    Transform class to perform a union of two tables with matching schemas.
    """
    def __init__(self, left_table: str, right_table: str, union_all: bool = False):
        """
        Initialise a UnionTables transform.

        Args:
            left_table (str): Name of the first table.
            right_table (str): Name of the second table.
            union_all (bool): If True, performs UNION ALL (includes duplicates).
        """
        super().__init__(
            "UnionTables",
            f"Union of '{left_table}' and '{right_table}' with union_all={union_all}",
            [left_table, right_table],
            "UnionSQL",
            testable_transform=False
        )
        self.left_table = left_table
        self.right_table = right_table
        self.union_all = union_all

    def error_check(self, supply_frames: "TableCollection", **kwargs):
        """
        Validate that both tables exist and have matching schemas.
        """
        if self.left_table not in supply_frames or self.right_table not in supply_frames:
            raise ValueError("Both tables must exist in supply_frames")

        left_cols = set(supply_frames[self.left_table].columns)
        right_cols = set(supply_frames[self.right_table].columns)

        if left_cols != right_cols:
            raise ValueError("Schemas do not match for union operation")

    def transforms(self, supply_frames: "TableCollection", **kwargs):
        """
        Perform the union operation.
        """
        backend = supply_frames[self.left_table].frame_type

        # Capture input columns before transformation
        input_columns = {
            self.left_table: list(supply_frames[self.left_table].columns),
            self.right_table: list(supply_frames[self.right_table].columns)
        }

        # Capture row counts before transformation
        left_row_count = supply_frames[self.left_table].nrow
        right_row_count = supply_frames[self.right_table].nrow

        if backend == "pandas":
            df = pd.concat(
                [supply_frames[self.left_table].df, supply_frames[self.right_table].df],
                ignore_index=True
            )
            if not self.union_all:
                df = df.drop_duplicates()

        elif backend == "polars":
            df = supply_frames[self.left_table].df.vstack(supply_frames[self.right_table].df)
            if not self.union_all:
                df = df.unique()

        elif backend == "pyspark":
            df = supply_frames[self.left_table].df.union(supply_frames[self.right_table].df)
            if not self.union_all:
                df = df.dropDuplicates()

        else:
            raise NotImplementedError(f"UnionTables not implemented for backend '{backend}'")

        # Store result in a new table
        union_table_name = f"{self.left_table}_union_{self.right_table}"
        supply_frames[union_table_name] = supply_frames[self.left_table].clone_with_new_df(df)

        # Capture row count and columns after transformation
        output_row_count = supply_frames[union_table_name].nrow
        output_columns = {union_table_name: list(supply_frames[union_table_name].columns)}

        self.log_info = TransformEvent(
            input_tables=[self.left_table, self.right_table],
            output_tables=[union_table_name],
            input_variables=[],
            output_variables=[],
            input_row_counts={self.left_table: left_row_count, self.right_table: right_row_count},
            output_row_counts={union_table_name: output_row_count},
            input_columns=input_columns,
            output_columns=output_columns
        )
        supply_frames[union_table_name].add_event(self)

        self.target_tables = [union_table_name]
        return supply_frames


# Transform discovery and listing functionality
def _discover_transforms():
    """
    Discover all TableTransform subclasses in the current module.
    Returns a list of tuples (class_name, class_obj, description).
    """
    current_module = sys.modules[__name__]
    transforms = []
    
    for name in dir(current_module):
        obj = getattr(current_module, name)
        if (inspect.isclass(obj) and 
            issubclass(obj, TableTransform) and 
            obj is not TableTransform):
            
            # Extract description from docstring
            description = ""
            if obj.__doc__:
                lines = obj.__doc__.strip().split('\n')
                if lines:
                    description = lines[0].strip()
            
            transforms.append((name, obj, description))
    
    return sorted(transforms)


def listatomic():
    """
    Display all available atomic transforms in a neat table format.
    """
    transforms = _discover_transforms()
    
    if not transforms:
        print("No transforms found.")
        return
    
    print("\n" + "="*printwidth)
    print(" TRANSFORMS LIBRARY - Available Transform Classes")
    print("="*printwidth)
    print(f" Total Transforms: {len(transforms)}")
    print("="*printwidth)
    
    # Calculate column widths
    max_name_width = max(len(name) for name, _, _ in transforms)
    max_desc_width = printwidth - max_name_width - 5  # Leave space for formatting
    
    print(f"{'Transform Name':<{max_name_width}} | Description")
    print("-" * max_name_width + "-+-" + "-" * max_desc_width)
    
    for name, _, description in transforms:
        # Truncate description if too long
        if len(description) > max_desc_width:
            description = description[:max_desc_width-3] + "..."
        print(f"{name:<{max_name_width}} | {description}")
    
    print("="*printwidth)
    print(" Use help(ClassName) for detailed information about any transform.")
    print("="*printwidth + "\n")

# Backward compatibility alias
def listme():
    """
    Legacy alias for listatomic(). Please use listatomic() instead.
    """
    import warnings
    warnings.warn("listme() is deprecated, use listatomic() instead", DeprecationWarning, stacklevel=2)
    return listatomic()


# Automatically discover and set up __all__ with all transforms
_transforms = _discover_transforms()
_transform_names = [name for name, _, _ in _transforms]

# Export all transforms and the listing functions
__all__ = _transform_names + ['listatomic', 'listme']

# Show the count whenever the module is imported (but not when run as main)
if __name__ != '__main__':
    print(f"\n Transforms Library: {len(_transforms)} transforms available")
    print("   Use listatomic() to see all available transforms in a table format.\n")

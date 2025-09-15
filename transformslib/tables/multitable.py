import os

from typing import Union
import polars as pl
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import concat_ws, col, explode, explode_outer, split
import sparkpolars as sp
from sas_to_polars import sas_to_polars

#module imports
from transformslib.tables.names.tablename import Tablename
from uainepydat.frameverifier import FrameTypeVerifier

def _load_spark_df(path:str, format: str = "parquet", table_name: str = "", spark=None) -> SparkDataFrame:
    """
    Load a Spark DataFrame from the given path and return a MetaFrame.
    """
    if spark is None:
        raise ValueError("SparkSession required for PySpark")

    if format == "sas":
        return spark.read.format("com.github.saurfang.sas.spark").load(path)
    elif format == "csv":
        return spark.read.format("csv").option("header", "true").load(path)
    else:
        return spark.read.format(format).load(path)

def _load_pandas_df(path:str, format: str = "parquet", table_name: str = "") -> pd.DataFrame:
    """
    Load a Pandas DataFrame from the given path and return a MetaFrame.
    """
    if format == "parquet":
        return pd.read_parquet(path)
    elif format == "csv":
        return pd.read_csv(path)  # Default: header inferred
    elif format == "sas":
        return pd.read_sas(path)
    else:
        raise ValueError("Unsupported format for pandas")

def _load_polars_df(path:str, format: str = "parquet", table_name: str = "") -> pl.LazyFrame:
    if format == "parquet":
        return pl.scan_parquet(path)
    elif format == "csv":
        return pl.scan_csv(path)  # Default: header inferred
    elif format == "sas":
        return sas_to_polars(path)
    else:
       raise ValueError("Unsupported format for polars")

class MultiTable: 
    """
    A unified wrapper class for handling DataFrames across different frameworks.
    
    This class provides a consistent interface for working with DataFrames from PySpark, Pandas, 
    and Polars. It includes utility methods for accessing DataFrame properties and converting between formats.
    
    Attributes:
        df: The underlying DataFrame (PySpark DataFrame, Pandas DataFrame, or Polars LazyFrame).
        frame_type (str): The type of DataFrame ('pyspark', 'pandas', 'polars').
        table_name (Tablename): The name of the table, validated and formatted.
        src_path (str): The source file path where the data was loaded from.
        
    Example:
        >>> # Create from existing DataFrame
        >>> import pandas as pd
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        >>> mf = MetaFrame(df, "data.csv", "my_table", "pandas")
        >>> 
        >>> # Load from file
        >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pyspark", spark)
        >>> 
        >>> # Access properties
        >>> print(f"Columns: {mf.columns}")
        >>> print(f"Rows: {mf.nrow}")
        >>> print(f"Variables: {mf.nvars}")
    """

    def sort_columns(self):
        """
        sort columns alphabetically in-place. Defaults to True.
        """
        # Inline sort for each frame type
        if self.frame_type == "pandas":
            self.df = self.df[sorted(self.df.columns)]
        elif self.frame_type == "polars":
            self.df = self.df.select(sorted(self.df.columns))
        elif self.frame_type == "pyspark":
            self.df = self.df.select(*sorted(self.df.columns))
        return self
    
    @staticmethod
    def infer_table_name(src_path: str) -> str:
        """
        Infer a table name from a file path by extracting the filename without extension.
        
        This method takes a file path and extracts the base filename, removing any file
        extensions to create a valid table name.

        Args:
            src_path (str): The source file path to extract the table name from.

        Returns:
            str: A Tablename object representing the inferred table name.

        Raises:
            ValueError: If the source path is empty.

        Example:
            >>> name = MetaFrame.infer_table_name("/path/to/my_data.csv")
            >>> print(name)  # "my_data"
            >>> 
            >>> name = MetaFrame.infer_table_name("/path/to/data.parquet")
            >>> print(name)  # "data"
        """
        if src_path == "":
            raise ValueError("Source path cannot be empty")

        #does it have a file extension?
        bn = os.path.basename(src_path)
        #print(bn)
        if bn.find(".") == -1:
            return Tablename(bn)
        else:
            return Tablename(os.path.splitext(bn)[0])

    def __init__(self, df: Union[pd.DataFrame, pl.DataFrame, SparkDataFrame], src_path: str = "", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark):
        """
        Initialise a MultiTable with a DataFrame and metadata.

        Args:
            df: The DataFrame to wrap (PySpark DataFrame, Pandas DataFrame, or Polars LazyFrame).
            src_path (str, optional): The source file path. Defaults to "".
            table_name (str, optional): The name for the table. If empty, will be inferred from src_path.
                                      Defaults to "".
            frame_type (str, optional): The type of DataFrame framework. Defaults to "pyspark".
                                      Supported types: "pyspark", "pandas", "polars".

        Raises:
            ValueError: If the DataFrame type doesn't match the specified frame_type.
            ValueError: If table_name is invalid when provided.

        Example:
            >>> import pandas as pd
            >>> df = pd.DataFrame({'A': [1, 2, 3]})
            >>> mf = MultiTable(df, "data.csv", "my_table", "pandas")
            >>> 
            >>> # Let table name be inferred
            >>> mf = MultiTable(df, "data.csv", frame_type="pandas")
        """
        #verify the frame type
        FrameTypeVerifier.verify(df, frame_type)

        #store the dataframe and type
        self.df = df
        self.frame_type = frame_type

        if table_name != "":
            self.table_name = Tablename(table_name)
        else:
            self.table_name = MultiTable.infer_table_name(src_path)

        self.src_path = src_path

    def __repr__(self):
        """
        Return a string representation of the MultiTable.
        
        Returns:
            str: The table name as a string representation.
        """
        return self.table_name

    def __str__(self):
        """
        Return a detailed string representation of the MultiTable.
        
        Returns:
            str: A JSON-like string with MultiTable metadata.
        """
        #JSON like string representation
        return f"MultiTable(name={self.table_name}, type={self.frame_type})"

    @property
    def columns(self):
        """
        Get the column names of the DataFrame.
        
        This property provides a unified way to access column names across different
        DataFrame frameworks.

        Returns:
            list: List of column names.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pandas")
            >>> print(mf.columns)  # ['col1', 'col2', 'col3']
        """
        if self.frame_type == "pyspark":
            return self.df.columns
        elif self.frame_type == "pandas":
            return list(self.df.columns)
        elif self.frame_type == "polars":
            return self.df.columns
        else:
            raise ValueError("Unsupported frame_type")

    @property
    def nvars(self):
        """
        Get the number of variables (columns) in the DataFrame.
        
        This property provides a unified way to get the column count across different
        DataFrame frameworks.

        Returns:
            int: Number of columns in the DataFrame.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MultiTable.load("data.parquet", "parquet", "my_table", "pandas")
            >>> print(f"Number of variables: {mf.nvars}")
        """
        if self.frame_type == "pyspark":
            return len(self.df.columns)
        elif self.frame_type == "pandas":
            return len(self.df.columns)
        elif self.frame_type == "polars":
            return len(self.df.columns)
        else:
            raise ValueError("Unsupported frame_type")

    @property
    def nrow(self):
        """
        Get the number of rows in the DataFrame.
        
        This property provides a unified way to get the row count across different
        DataFrame frameworks. Note that for PySpark, this triggers an action.

        Returns:
            int: Number of rows in the DataFrame.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MultiTable.load("data.parquet", "parquet", "my_table", "pandas")
            >>> print(f"Number of rows: {mf.nrow}")
        """
        if self.frame_type == "pyspark":
            return self.df.count()
        elif self.frame_type == "pandas":
            return len(self.df)
        elif self.frame_type == "polars":
            return self.df.height
        else:
            raise ValueError("Unsupported frame_type")

    def get_pandas_frame(self):
        """
        Convert the DataFrame to a Pandas DataFrame.
        
        This method provides a unified way to convert any supported DataFrame type
        to a Pandas DataFrame for analysis or processing.

        Returns:
            pd.DataFrame: A Pandas DataFrame representation of the data.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pyspark", spark)
            >>> pandas_df = mf.get_pandas_frame()
            >>> print(type(pandas_df))  # <class 'pandas.core.frame.DataFrame'>
        """
        if self.frame_type == "pyspark":
            return self.df.toPandas()
        elif self.frame_type == "pandas":
            print("WARNING: Unoptimised code, DataFrame is already a Pandas DataFrame.")
            return self.df
        elif self.frame_type == "polars":
            return self.df.to_pandas()
        else:
            raise ValueError("Unsupported frame_type")

    def get_polars_lazy_frame(self):
        """
        Convert the DataFrame to a Polars LazyFrame.
        
        This method provides a unified way to convert any supported DataFrame type
        to a Polars LazyFrame for lazy evaluation and optimization.

        Returns:
            pl.LazyFrame: A Polars LazyFrame representation of the data.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pandas")
            >>> lazy_frame = mf.get_polars_lazy_frame()
            >>> print(type(lazy_frame))  # <class 'polars.lazyframe.frame.LazyFrame'>
        """
        if self.frame_type == "pyspark":
            lf = sp.from_spark(self.df)
            return lf.lazy()
        elif self.frame_type == "polars":
            print("WARNING: Unoptimised code, DataFrame is already a polars LazyFrame.")
            return self.df
        elif self.frame_type == "pandas":
            return pl.from_pandas(self.df).lazy()
        else:
            raise ValueError("Unsupported frame_type")

    def show(self, n: int = 20, truncate: bool = True):
        """
        Display the DataFrame content in a formatted way.
        
        This method provides a unified way to display DataFrame content across different
        frameworks. It handles the different display behaviors of PySpark, Pandas, and Polars.

        Args:
            n (int, optional): Number of rows to display. Defaults to 20.
            truncate (bool, optional): Whether to truncate long strings. Defaults to True.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pandas")
            >>> mf.show()  # Display first 20 rows
            >>> mf.show(10)  # Display first 10 rows
            >>> mf.show(50, truncate=False)  # Display first 50 rows without truncation
        """
        if self.frame_type == "pyspark":
            self.df.show(n=n, truncate=truncate)
        elif self.frame_type == "pandas":
            print(f"MultiTable: {self.table_name} ({self.frame_type})")
            print(f"Shape: {self.df.shape[0]} rows × {self.df.shape[1]} columns")
            print(self.df.head(n))
        elif self.frame_type == "polars":
            print(f"MultiTable: {self.table_name} ({self.frame_type})")
            # For Polars LazyFrame, we need to collect first
            collected_df = self.df.collect()
            print(f"Shape: {collected_df.shape[0]} rows × {collected_df.shape[1]} columns")
            print(collected_df.head(n))
        else:
            raise ValueError("Unsupported frame_type")

    @staticmethod
    def load_native_df(path:str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, spark=None):
        """
        Load a DataFrame from a file and return a DataFrame instance.

        This static method provides a convenient way to load data from various file formats and create a dataframe. It supports multiple file formats and DataFrame frameworks.

        Parameters
        ----------
        path : str
            Path to the data file to load.
        format : str, optional
            File format of the data. Defaults to "parquet". Supported formats: "parquet", "csv", "sas".
        table_name : str, optional
            Name to assign to the table. If empty, will be inferred from the file path. Defaults to "".
        frame_type : str, optional
            Type of DataFrame to create. Defaults to "pyspark". Supported types: "pyspark", "pandas", "polars".
        spark : optional
            SparkSession object (required for PySpark frame_type). Defaults to None.

        Returns
        -------
        dataframe : pyspark.sql.DataFrame or pandas.DataFrame or polars.LazyFrame
            A pyspark, polars or pandas dataframe.

        Raises
        ------
        FileNotFoundError
            If the specified path does not exist.
        ValueError
            If the format or frame_type is not supported, or if spark is None when frame_type is "pyspark".
        Exception
            If there are issues loading the data.
        """
        if frame_type == "pyspark":
            df = _load_spark_df(path, format, table_name, spark)
        elif frame_type == "pandas":
            df = _load_pandas_df(path, format, table_name)
        elif frame_type == "polars":
            df = _load_polars_df(path, format, table_name)
        else:
            raise ValueError("Unsupported frame_type")
        return df

    @staticmethod
    def load(path:str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, auto_capitalise = False, spark=None):
        """
        Load a DataFrame from a file and return a MultiTable instance.

        This static method provides a convenient way to load data from various file formats and create a MultiTable with appropriate metadata. It supports multiple file formats and DataFrame frameworks.

        Parameters
        ----------
        path : str
            Path to the data file to load.
        format : str, optional
            File format of the data. Defaults to "parquet". Supported formats: "parquet", "csv", "sas".
        table_name : str, optional
            Name to assign to the table. If empty, will be inferred from the file path. Defaults to "".
        frame_type : str, optional
            Type of DataFrame to create. Defaults to "pyspark". Supported types: "pyspark", "pandas", "polars".
        auto_capitalise : bool, optional
            Automatically capitalise column names. Defaults to False.
        spark : optional
            SparkSession object (required for PySpark frame_type). Defaults to None.

        Returns
        -------
        MultiTable
            A new MultiTable instance with the loaded data and metadata.

        Raises
        ------
        FileNotFoundError
            If the specified path does not exist.
        ValueError
            If the format or frame_type is not supported, or if spark is None when frame_type is "pyspark".
        Exception
            If there are issues loading the data.

        Examples
        --------
        >>> # Load a PySpark DataFrame
        >>> mf = MultiTable.load("data.parquet", format="parquet", table_name="my_table", frame_type="pyspark", spark=spark)
        >>> # Load a Pandas DataFrame
        >>> mf = MultiTable.load("data.csv", format="csv", table_name="my_table", frame_type="pandas")
        >>> # Load a Polars DataFrame
        >>> mf = MultiTable.load("data.parquet", format="parquet", table_name="my_table", frame_type="polars")
        """
        df = MultiTable.load_native_df(path=path, format=format, table_name=table_name, frame_type=frame_type, spark=spark)

        if auto_capitalise:
            if frame_type == "pandas":
                df.columns = [col.upper() for col in df.columns]  # or .title() if you prefer
            elif frame_type == "polars":
                df = df.rename({col: col.upper() for col in df.columns})  # returns a new LazyFrame
            elif frame_type == "pyspark":
                for old_col in df.columns:
                    df = df.withColumnRenamed(old_col, old_col.upper())
            else:
                raise ValueError("Unsupported frame_type for auto_capitalise")

        #print(df.columns)
        #package into metaframe
        mf = MultiTable(df, src_path=path, table_name=table_name, frame_type=frame_type)
            
        return mf

    def drop(self, columns: Union[str, list]):
        """
        Drop one or more columns from the DataFrame.
        
        Args:
            columns (Union[str, list]): Column name or list of column names to drop.
        
        Returns:
            MultiTable: A new MultiTable instance with the specified columns removed.
        
        Raises:
            ValueError: If the frame_type is unsupported.
        
        Example:
            >>> mf2 = mf.drop("col1")  # Drop a single column
            >>> mf3 = mf.drop(["col1", "col2"])  # Drop multiple columns
        """
        if isinstance(columns, str):
            columns = [columns]

        if self.frame_type == "pandas":
            new_df = self.df.drop(columns=columns)
        elif self.frame_type == "polars":
            new_df = self.df.drop(columns)  # LazyFrame drop supports list of columns
        elif self.frame_type == "pyspark":
            new_df = self.df
            for col in columns:
                new_df = new_df.drop(col)
        else:
            raise ValueError("Unsupported frame_type")

        self.df = new_df

    def distinct(self, subset: Union[str, list, None] = None):
        """
        Return a new MultiTable with distinct rows.
        
        Args:
            subset (Union[str, list, None], optional): 
                Column name or list of column names to consider for distinct.
                If None, considers all columns.
        
        Returns:
            MultiTable: A new MultiTable with distinct rows.
        
        Raises:
            ValueError: If the frame_type is unsupported.
        
        Example:
            >>> mf2 = mf.distinct()  # distinct across all columns
            >>> mf3 = mf.distinct("col1")  # distinct by one column
            >>> mf4 = mf.distinct(["col1", "col2"])  # distinct by multiple columns
        """
        if isinstance(subset, str):
            subset = [subset]

        if self.frame_type == "pandas":
            new_df = self.df.drop_duplicates(subset=subset)
        elif self.frame_type == "polars":
            if subset is None:
                new_df = self.df.unique()
            else:
                new_df = self.df.unique(subset)
        elif self.frame_type == "pyspark":
            if subset is None:
                new_df = self.df.distinct()
            else:
                new_df = self.df.dropDuplicates(subset)
        else:
            raise ValueError("Unsupported frame_type")

        self.df = new_df
        
        return self

    def rename(self, columns: dict, inplace: bool = True):
        """
        Rename columns in the DataFrame.

        Args:
            columns (dict): Mapping from old column names to new column names.
            inplace (bool): If True, modifies the current MultiTable. If False, returns a new MultiTable.

        Returns:
            MultiTable or None: Returns a new MultiTable if inplace=False, else None.
        """
        if self.frame_type == "pandas":
            new_df = self.df.rename(columns=columns)
        elif self.frame_type == "polars":
            new_df = self.df.rename(columns)
        elif self.frame_type == "pyspark":
            new_df = self.df
            for old_col, new_col in columns.items():
                new_df = new_df.withColumnRenamed(old_col, new_col)
        else:
            raise ValueError("Unsupported frame_type")

        if inplace:
            self.df = new_df
            return None
        else:
            return MultiTable(new_df, src_path=self.src_path, table_name=str(self.table_name), frame_type=self.frame_type)

    @staticmethod
    def write_native_df(dataframe, path:str, format: str = "parquet", frame_type: str = FrameTypeVerifier.pyspark, overwrite: bool = True, spark=None):
        """
        Write a DataFrame to a file in the specified format.
        """
        if frame_type == "pyspark":
            mode = "overwrite" if overwrite else "error"
            print(f"Writing to {path} as {format} with mode={mode} (compression=zstd)")
            if format == "parquet":
                dataframe.write.mode(mode).option("compression", "zstd").format(format).save(path)
            else:
                dataframe.write.mode(mode).format(format).save(path)
        elif frame_type == "pandas":
            if os.path.exists(path) and not overwrite:
                raise FileExistsError(f"File {path} already exists and overwrite is False.")
            if format == "parquet":
                dataframe.to_parquet(path, index=False, compression="zstd")
            else:
                dataframe.to_parquet(path, index=False)
        elif frame_type == "polars":
            if os.path.exists(path) and not overwrite:
                raise FileExistsError(f"File {path} already exists and overwrite is False.")
            if format == "parquet":
                dataframe.sink_parquet(path, compression="zstd", compression_level=1)
            else:
                dataframe.sink_parquet(path)

    def write(self, path:str, format: str = "parquet", overwrite: bool = True, spark=None):
        """
        Write the DataFrame to a file in the specified format.

        Parameters
        ----------
        path : str
            Destination file path.
        format : str, optional
            File format to write. Defaults to "parquet". Supported formats: "parquet", "csv", "sas" (for PySpark).
        overwrite : bool, optional
            If True, overwrites existing files. Defaults to True.
        spark : optional
            SparkSession object (required for PySpark frame_type). Defaults to None.

        Raises
        ------
        ValueError
            If the frame_type is unsupported.
        FileExistsError
            If the file exists and overwrite is False.
        """
        MultiTable.write_native_df(self.df, path, format, self.frame_type, overwrite, spark)
        
    def concat(self, new_col_name: str, columns: list, sep: str = "_"):
        """
        Concatenate multiple columns into a single column with a custom separator,
        modifying the current MultiTable in-place.

        Args:
            new_col_name (str): Name of the resulting concatenated column.
            columns (list): List of column names to concatenate.
            sep (str, optional): Separator to use between values. Defaults to "_".

        Raises:
            ValueError: If the frame_type is unsupported.
        """
        if self.frame_type == "pandas":
            self.df[new_col_name] = self.df[columns].astype(str).agg(sep.join, axis=1)

        elif self.frame_type == "polars":
            exprs = [pl.col(column_name).cast(pl.Utf8) for column_name in columns]
            new_expr = pl.concat_str(exprs, separator=sep).alias(new_col_name)
            self.df = self.df.with_columns(new_expr)

        elif self.frame_type == "pyspark":
            print(columns)
            print(sep)
            new_expr = concat_ws(sep, *[col(column_name).cast("string") for column_name in columns])
            self.df = self.df.withColumn(new_col_name, new_expr)

        else:
            raise ValueError("Unsupported frame_type for concat")

    def explode(self, column: str, sep: str | None = None, outer: bool = False):
        """
        Explode (flatten) a column containing lists/arrays (or delimited strings) into multiple rows.
        Always modifies the current MultiTable in place.

        Args:
            column (str): Column name to explode.
            sep (str | None): If provided, split strings in the column by this separator before exploding.
            outer (bool): If True, performs an 'outer explode' (keeps rows where the column is null/empty).

        Raises:
            ValueError: If the frame_type is unsupported.
        """
        if self.frame_type == "pandas":
            if sep:
                self.df[column] = self.df[column].str.split(sep)

            if outer:
                # Pandas explode already keeps NaN, so it's effectively outer
                self.df = self.df.explode(column, ignore_index=True)
            else:
                # dropna ensures we mimic non-outer explode
                self.df = self.df.explode(column, ignore_index=True).dropna(subset=[column])

        elif self.frame_type == "polars":
            if sep:
                self.df = self.df.with_columns(pl.col(column).str.split(sep))

            if outer:
                # Polars explode keeps nulls, so same as outer
                self.df = self.df.with_columns(pl.col(column).explode())
            else:
                # filter out nulls to mimic non-outer explode
                self.df = (
                    self.df.filter(pl.col(column).is_not_null())
                    .with_columns(pl.col(column).explode())
                )

        elif self.frame_type == "pyspark":
            if sep:
                self.df = self.df.withColumn(column, split(col(column), sep))

            if outer:
                self.df = self.df.withColumn(column, explode_outer(col(column)))
            else:
                self.df = self.df.withColumn(column, explode(col(column)))

        else:
            raise ValueError("Unsupported frame_type for explode")

        return None
    
    from typing import Union, List

    def sort(self, by: Union[str, List[str]], ascending: Union[bool, List[bool]] = True) -> "MultiTable":
        """
        Sort the DataFrame by one or more columns in-place.
        Works with pandas, polars, and pyspark DataFrames.

        Args:
            by (str | list[str]): Column name or list of column names to sort by.
            ascending (bool | list[bool], optional): Sort order.
                True for ascending, False for descending.
                Can be a single bool or a list matching the columns. Defaults to True.

        Returns:
            MultiTable: The current MultiTable instance with sorted data (for chaining).

        Raises:
            ValueError: If the length of 'ascending' does not match length of 'by',
                        or if the frame_type is unsupported.

        Example:
            >>> mt.sort("col1")
            >>> mt.sort(["col1", "col2"], ascending=[True, False])
            >>> mt.sort("col1").sort("col2", ascending=False)  # chaining
        """
        if isinstance(by, str):
            by = [by]
        if isinstance(ascending, bool):
            ascending = [ascending] * len(by)
        if len(ascending) != len(by):
            raise ValueError("Length of 'ascending' must match length of 'by' columns.")

        if self.frame_type == "pandas":
            self.df = self.df.sort_values(by=by, ascending=ascending)
        elif self.frame_type == "polars":
            self.df = self.df.sort(by, reverse=[not asc for asc in ascending])
        elif self.frame_type == "pyspark":
            sort_cols = [col(c) if asc else col(c).desc() for c, asc in zip(by, ascending)]
            self.df = self.df.orderBy(*sort_cols)
        else:
            raise ValueError("Unsupported frame_type for sort")

        return self

    def sample(self, n: int = None, frac: float = None, seed: int = None):
        """
        Sample rows from the DataFrame and replace the existing DataFrame inplace.

        Args:
            n (int, optional): Number of rows to sample. Mutually exclusive with `frac`.
            frac (float, optional): Fraction of rows to sample. Mutually exclusive with `n`.
            seed (int, optional): Random seed for reproducibility.

        Returns:
            None
        """
        if n is not None and frac is not None:
            raise ValueError("Specify either `n` or `frac`, not both.")

        if self.frame_type == FrameTypeVerifier.pandas:
            self.df = self.df.sample(n=n, frac=frac, random_state=seed)
        elif self.frame_type == FrameTypeVerifier.polars:
            if frac is not None:
                self.df = self.df.sample(frac=frac, seed=seed)
            else:
                self.df = self.df.sample(n=n, seed=seed)
        elif self.frame_type == FrameTypeVerifier.pyspark:
            if frac is None:
                if n is None:
                    raise ValueError("Must specify either `n` or `frac` for sampling.")
                frac = n / self.df.count()
            self.df = self.df.sample(withReplacement=False, fraction=frac, seed=seed)
        else:
            raise NotImplementedError(f"Sampling not supported for frame type {self.frame_type}")
    
    @property
    def dtypes(self) -> dict:
        """
        Get the data types of each column in the DataFrame.

        Returns:
            dict: A dictionary where keys are column names and values are their data types.

        Raises:
            ValueError: If the frame_type is unsupported.

        Example:
            >>> mt.dtypes
            {'col1': 'int64', 'col2': 'string', ...}
        """
        if self.frame_type == "pandas":
            return self.df.dtypes.apply(lambda dtype: dtype.name).to_dict()
        
        elif self.frame_type == "polars":
            return {col: str(self.df.schema[col]) for col in self.df.columns}
        
        elif self.frame_type == "pyspark":
            return {field.name: str(field.dataType) for field in self.df.schema.fields}
        
        else:
            raise ValueError("Unsupported frame_type")

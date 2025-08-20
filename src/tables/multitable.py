import os

from typing import Union
import polars as pl
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
import sparkpolars as sp
from sas_to_polars import sas_to_polars

#module imports
from tables.names.tablename import Tablename
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
        metaframe_version (str): Version identifier for the MetaFrame implementation.
        
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
        Load a DataFrame from a file and return a MultiTable instance.
        
        This static method provides a convenient way to load data from various file formats
        and create a dataframe. It supports multiple file formats
        and DataFrame frameworks.

        Args:
            path (str): Path to the data file to load.
            format (str, optional): File format of the data. Defaults to "parquet".
                                  Supported formats: "parquet", "csv", "sas".
            table_name (str, optional): Name to assign to the table. If empty, will be
                                      inferred from the file path. Defaults to "".
            frame_type (str, optional): Type of DataFrame to create. Defaults to "pyspark".
                                      Supported types: "pyspark", "pandas", "polars".
            spark: SparkSession object (required for PySpark frame_type). Defaults to None.

        Returns:
            dataframe: A pyspark, polars or pandas dataframe.

        Raises:
            FileNotFoundError: If the specified path does not exist.
            ValueError: If the format or frame_type is not supported.
            ValueError: If spark is None when frame_type is "pyspark".
            Exception: If there are issues loading the data.

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
    def load(path:str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, spark=None):
        """
        Load a DataFrame from a file and return a MultiTable instance.
        
        This static method provides a convenient way to load data from various file formats
        and create a MultiTable with appropriate metadata. It supports multiple file formats
        and DataFrame frameworks.

        Args:
            path (str): Path to the data file to load.
            format (str, optional): File format of the data. Defaults to "parquet".
                                  Supported formats: "parquet", "csv", "sas".
            table_name (str, optional): Name to assign to the table. If empty, will be
                                      inferred from the file path. Defaults to "".
            frame_type (str, optional): Type of DataFrame to create. Defaults to "pyspark".
                                      Supported types: "pyspark", "pandas", "polars".
            spark: SparkSession object (required for PySpark frame_type). Defaults to None.

        Returns:
            MultiTable: A new MultiTable instance with the loaded data and metadata.

        Raises:
            FileNotFoundError: If the specified path does not exist.
            ValueError: If the format or frame_type is not supported.
            ValueError: If spark is None when frame_type is "pyspark".
            Exception: If there are issues loading the data.

        Example:
            >>> # Load a PySpark DataFrame
            >>> mf = MultiTable.load("data.parquet", "parquet", "my_table", "pyspark", spark)
            >>> 
            >>> # Load a Pandas DataFrame
            >>> mf = MultiTable.load("data.csv", "csv", "my_table", "pandas")
            >>> 
            >>> # Load a Polars DataFrame
            >>> mf = MultiTable.load("data.parquet", "parquet", "my_table", "polars")
        """
        df = MultiTable.load_native_df(path=path, format=format, table_name=table_name, frame_type=frame_type, spark=spark)
        #package into metaframe
        mf = MultiTable(df, src_path=path, table_name=table_name, frame_type=frame_type)
        return mf

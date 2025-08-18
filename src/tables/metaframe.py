from tables.multitable import MultiTable

from typing import Union
import polars as pl
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from uainepydat.frameverifier import FrameTypeVerifier

class MetaFrame(MultiTable): 
    """
    A unified wrapper class for handling DataFrames with metadata across different frameworks.
    
    This class provides a consistent interface for working with DataFrames from PySpark, Pandas, 
    and Polars, while maintaining metadata about the source, table name, and framework type.
    It includes utility methods for accessing DataFrame properties and converting between formats.
    
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

    def __init__(self, df: Union[pd.DataFrame, pl.DataFrame, SparkDataFrame], src_path: str = "", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark):
        """
        Initialize a MetaFrame with a DataFrame and metadata.

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
            >>> mf = MetaFrame(df, "data.csv", "my_table", "pandas")
            >>> 
            >>> # Let table name be inferred
            >>> mf = MetaFrame(df, "data.csv", frame_type="pandas")
        """
        super().__init__(df, src_path=src_path, table_name=table_name, frame_type=frame_type)

        self.metaframe_version = "0.1.0"

    @staticmethod
    def load(path:str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, spark=None):
        """
        Load a DataFrame from a file and return a MetaFrame instance.
        
        This static method provides a convenient way to load data from various file formats
        and create a MetaFrame with appropriate metadata. It supports multiple file formats
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
            MetaFrame: A new MetaFrame instance with the loaded data and metadata.

        Raises:
            FileNotFoundError: If the specified path does not exist.
            ValueError: If the format or frame_type is not supported.
            ValueError: If spark is None when frame_type is "pyspark".
            Exception: If there are issues loading the data.

        Example:
            >>> # Load a PySpark DataFrame
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pyspark", spark)
            >>> 
            >>> # Load a Pandas DataFrame
            >>> mf = MetaFrame.load("data.csv", "csv", "my_table", "pandas")
            >>> 
            >>> # Load a Polars DataFrame
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "polars")
        """
        df = MultiTable.load_native_df(path=path, format=format, table_name=table_name, frame_type=frame_type, spark=spark)

        #package into metaframe
        mf = MetaFrame(df, src_path=path, table_name=table_name, frame_type=frame_type)
        return mf

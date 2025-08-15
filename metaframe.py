import os

from typing import Union
import polars as pl
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
import sparkpolars as sp
from sas_to_polars import sas_to_polars

#module imports
from tablename import Tablename
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

class MetaFrame: 
    """
    Class to handle the Metadata with a dataframe.
    Supports PySpark, Pandas, or Polars DataFrames.
    """

    @staticmethod
    def infer_table_name(src_path: str) -> str:
        """
        Returns the table name from the filepath, removing any file extensions
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
        Initialize the MetaplusTable with a DataFrame and type.

        :param df: DataFrame (PySpark, Pandas, or Polars).
        :param src_path: Optional source file path.
        :param table_name: Optional table name.
        :param frame_type: Type of DataFrame ('pyspark', 'pandas', 'polars').
        """
        #verify the frame type
        FrameTypeVerifier.verify(df, frame_type)

        #store the dataframe and type
        self.df = df
        self.frame_type = frame_type

        if table_name != "":
            self.table_name = Tablename(table_name)
        else:
            self.table_name = MetaFrame.infer_table_name(src_path)

        self.src_path = src_path

        self.metaframe_version = "0.1.0"

    def __repr__(self):
        return self.table_name

    def __str__(self):
        #JSON like string representation
        return f"MetaFrame(name={self.table_name}, type={self.frame_type})"

    @property
    def columns(self):
        """
        Get the column names of the DataFrame.
        
        :return: List of column names.
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
        
        :return: Number of columns.
        """
        if self.frame_type == "pyspark":
            return len(self.df.columns)
        elif self.frame_type == "pandas":
            return len(self.df.columns)
        elif self.frame_type == "polars":
            return len(self.df.columns)
        else:
            raise ValueError("Unsupported frame_type")

    def get_pandas_frame(self):
        """
        Convert the DataFrame to a Pandas DataFrame.

        :return: Pandas DataFrame.
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

        :return: Polars LazyFrame.
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

    @staticmethod
    def load(path:str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, spark=None):
        """
        Load a DataFrame from the given path and return a MetaFrame.

        :param path: Path to the data file.
        :param format: File format (default: 'parquet').
        :param table_name: Optional table name.
        :param frame_type: Type of DataFrame ('pyspark', 'pandas', 'polars').
        :param spark: SparkSession object (required for PySpark).
        :return: MetaFrame instance.
        """
        if frame_type == "pyspark":
            df = _load_spark_df(path, format, table_name, spark)
        elif frame_type == "pandas":
            df = _load_pandas_df(path, format, table_name)
        elif frame_type == "polars":
            df = _load_polars_df(path, format, table_name)
        else:
            raise ValueError("Unsupported frame_type")

        #package into metaframe
        mf = MetaFrame(df, src_path=path, table_name=table_name, frame_type=frame_type)
        return mf

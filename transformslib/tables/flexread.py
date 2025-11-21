import polars as pl
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
import polars_readstat as prs
from deltalake import DeltaTable


def _load_spark_df(path:str, format: str = "parquet", table_name: str = "", spark=None) -> SparkDataFrame:
    """
    Load a Spark DataFrame from the given path and return a MetaFrame.
    """
    if spark is None:
        raise ValueError("MT020 SparkSession required for PySpark")

    if format == "sas":
        return spark.read.format("com.github.saurfang.sas.spark").load(path)
    elif format == "csv":
        return spark.read.format("csv").option("header", "true").load(path)
    elif format == "parquet" or format == "delta":
        return spark.read.format(format).load(path)
    else:
        raise ValueError("MT002 Unsupported format for pyspark")

def _load_pandas_df(path:str, format: str = "parquet", table_name: str = "") -> pd.DataFrame:
    """
    Load a Pandas DataFrame from the given path and return a MetaFrame.
    """
    if format == "parquet":
        return pd.read_parquet(path)
    elif format == "delta":
        dt = DeltaTable(path)
        return dt.to_pandas()
    elif format == "csv":
        return pd.read_csv(path)  # Default: header inferred
    elif format == "sas":
        return pd.read_sas(path)
    else:
        raise ValueError("MT001 Unsupported format for pandas")

def _load_polars_df(path:str, format: str = "parquet", table_name: str = "") -> pl.LazyFrame:
    if format == "parquet":
        return pl.scan_parquet(path)
    elif format == "delta":
        raise ValueError("MT003 Delta format not supported for polars. Use pandas instead.")
    elif format == "csv":
        return pl.scan_csv(path)  # Default: header inferred
    elif format == "sas":
        return prs.scan_readstat(path)
    else:
       raise ValueError("MT004 Unsupported format for polars")

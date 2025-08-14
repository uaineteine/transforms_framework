import os
from pipeline_event import PipelineEvent

from typing import Union
import polars as pl
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
import sparkpolars as sp
from sas_to_polars import sas_to_polars

#module imports
from tablename import Tablename
from uainepydat.frameverifier import FrameTypeVerifier

class Metaframe: 
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

        if src_path:
            #does it have a file extension?
            if src_path.find(".") != -1:
                self.table_name = Tablename(os.path.basename(src_path))
            else:
                self.table_name = Tablename(os.path.splitext(os.path.basename(src_path))[0])

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

        if table_name:
            self.table_name = Tablename(table_name)
        else:
            self.table_name = Metaframe.infer_table_name(src_path)

        self.src_path = src_path

    def __repr__(self):
        return self.table_name

    def __str__(self):
        return f"MetaplusTable(name={self.table_name}, type={self.frame_type})"

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
            return self.df.lazy()
        elif self.frame_type == "pandas":
            return pl.from_pandas(self.df).lazy()
        else:
            raise ValueError("Unsupported frame_type")

    @staticmethod
    def load(spark=None, path: str = "", format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark):
        """
        Load a DataFrame from the given path and return a MetaplusTable.

        :param spark: SparkSession object (required for PySpark).
        :param path: Path to the data file.
        :param format: File format (default: 'parquet').
        :param table_name: Optional table name.
        :param frame_type: Type of DataFrame ('pyspark', 'pandas', 'polars').
        :return: MetaplusTable instance.
        """
        if frame_type == "pyspark":
            if spark is None:
                raise ValueError("SparkSession required for PySpark")
            if format == "sas":
                df = spark.read.format("com.github.saurfang.sas.spark").load(path)
            elif format == "csv":
                df = spark.read.format("csv").option("header", "true").load(path)
            else:
                df = spark.read.format(format).load(path)
        elif frame_type == "pandas":
            if format == "parquet":
                df = pd.read_parquet(path)
            elif format == "csv":
                df = pd.read_csv(path)  # Default: header inferred
            elif format == "sas":
                df = pd.read_sas(path)
            else:
                raise ValueError("Unsupported format for pandas")
        elif frame_type == "polars":
            if format == "parquet":
                df = pl.read_parquet(path)
            elif format == "csv":
                df = pl.read_csv(path)  # Default: header inferred
            elif format == "sas":
                df = sas_to_polars(path)
            else:
                raise ValueError("Unsupported format for polars")
        else:
            raise ValueError("Unsupported frame_type")

        tbl = Metaframe(df, src_path=path, table_name=table_name, frame_type=frame_type)
        event = PipelineEvent(event_type="load", message=f"Loaded table from {path} as {format} ({frame_type})", description=f"Loaded {table_name} from {path}")
        tbl.events.append(event)
        return tbl

    def save_events(self):
        """
        Save the events to a JSON file in the events_log directory.

        :return: None
        """
        if not os.path.exists("events_log"):
            os.makedirs("events_log")
        
        log_path = f"events_log/job_1/{self.table_name}_events.json"
        for event in self.events:
            event.log_location = log_path
            event.log()
        
        print(f"Events saved to {log_path}")

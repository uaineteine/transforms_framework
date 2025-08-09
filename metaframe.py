import os
from frameverifier import FrameTypeVerifier
from pipeline_event import PipelineEvent

#frame types
import polars as pl
import pandas as pd
from sas_to_polars import sas_to_polars

class Metaframe:
    """
    Class to handle the MetaplusTable table.
    Supports PySpark, Pandas, or Polars DataFrames.
    """

    def __init__(self, df, src_path: str = "", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark):
        """
        Initialize the MetaplusTable with a DataFrame and type.

        :param df: DataFrame (PySpark, Pandas, or Polars).
        :param src_path: Optional source file path.
        :param table_name: Optional table name.
        :param frame_type: Type of DataFrame ('pyspark', 'pandas', 'polars').
        """
        FrameTypeVerifier.verify(df, frame_type)

        self.df = df
        self.frame_type = frame_type

        self.table_name = ""
        if table_name:
            self.table_name = table_name
        elif src_path:
            self.table_name = os.path.splitext(os.path.basename(src_path))[0]

        self.src_path = src_path
        self.events = []

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
        event = PipelineEvent(event_type="load", message=f"Loaded table from {path} as {format} ({frame_type})")
        tbl.events.append(event)
        return tbl

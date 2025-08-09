import os
from pipeline_event import PipelineEvent
from pyspark.sql import DataFrame

class MetaplusTable:
    """
    Class to handle the MetaplusTable table.
    """

    def __init__(self, df: DataFrame, src_path: str = "", table_name: str = ""):
        """
        Initialize the MetaplusTable with a primary DataFrame and optional src_path/table_name.

        :param df: Primary Spark DataFrame.
        :param src_path: Optional source file path.
        :param table_name: Optional table name.
        """
        self.df = df

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
        return f"MetaplusTable(name={self.table_name})"

    def get_pandas_frame(self):
        """
        Convert the Spark DataFrame to a Pandas DataFrame.

        :return: Pandas DataFrame.
        """
        return self.df.toPandas()

    @staticmethod
    def load(spark, path: str, format: str = "parquet", table_name: str = ""):
        """
        Load a DataFrame from the given path and return a MetaplusTable.

        :param spark: SparkSession object.
        :param path: Path to the data file.
        :param format: File format (default: 'parquet').
        :param table_name: Optional table name.
        :return: MetaplusTable instance.
        """
        df = spark.read.format(format).load(path)
        tbl = MetaplusTable(df, src_path=path, table_name=table_name)
        event = PipelineEvent(event_type="load", message=f"Loaded table from {path} as {format}")
        tbl.events.append(event)
        return tbl

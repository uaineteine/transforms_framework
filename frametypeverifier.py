class FrameTypeVerifier:
    """
    Static class to verify that the DataFrame matches the specified frame_type.
    """
    SUPPORTED_TYPES = ("pyspark", "pandas", "polars")
    polars="polars"
    pyspark="pyspark"
    pandas="pandas"

    @staticmethod
    def is_supported(frame_type: str) -> bool:
        """
        Check if the frame_type is supported.

        :param frame_type: Type of DataFrame ('pyspark', 'pandas', 'polars').
        :return: True if supported, False otherwise.
        """
        return frame_type in FrameTypeVerifier.SUPPORTED_TYPES

    @staticmethod
    def verify(df, frame_type:str):
        if frame_type not in FrameTypeVerifier.SUPPORTED_TYPES:
            raise ValueError(f"frame_type must be one of {FrameTypeVerifier.SUPPORTED_TYPES}")

        if frame_type == "pyspark":
            try:
                from pyspark.sql import DataFrame as SparkDataFrame
            except ImportError:
                SparkDataFrame = None
            if SparkDataFrame is None or not isinstance(df, SparkDataFrame):
                raise TypeError("df must be a PySpark DataFrame when frame_type is 'pyspark'")
        elif frame_type == "pandas":
            try:
                import pandas as pd
            except ImportError:
                pd = None
            if pd is None or not isinstance(df, pd.DataFrame):
                raise TypeError("df must be a Pandas DataFrame when frame_type is 'pandas'")
        elif frame_type == "polars":
            try:
                import polars as pl
            except ImportError:
                pl = None
            if not isinstance(df, pl.DataFrame):
                raise TypeError("df must be a Polars DataFrame when frame_type is 'polars'")

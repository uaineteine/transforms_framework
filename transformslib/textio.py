"""
txtread.py

Utility functions for reading and writing raw text files.
Supports both local file paths and abfss:// paths for Azure Data Lake Storage.
Uses Spark for distributed file operations when working with abfss:// paths.

Functions:
    read_raw_text(path: str, spark=None) -> str
        Read a text file and return its content as a single string.

    save_raw_text(path: str, text: str, spark=None)
        Save a string to a text file.

Author: Daniel
Created: September 2025
"""

def read_raw_text(path:str, spark=None) -> str:
    """Read a text file and return its content as a single string.

    Args:
        path (str): Path to the text file. Supports local paths and abfss:// paths.
        spark (SparkSession, optional): Spark session for reading from abfss:// paths. Defaults to None.
    """

    if path.startswith("abfss://"):
        if spark is None:
            raise ValueError("Spark session must be provided for reading from abfss:// paths.")
        df = spark.read.text(path)
        lines = df.rdd.map(lambda row: row[0]).collect()
        txt = "\n".join(lines)
        return txt
    else:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

def save_raw_text(path:str, text:str, spark=None):
    """Save a string to a text file.

    Args:
        path (str): Path to the text file. Supports local paths and abfss:// paths.
        text (str): The text content to save.
        spark (SparkSession, optional): Spark session for writing to abfss:// paths. Defaults to None.
    """

    if path.startswith("abfss://"):
        if spark is None:
            raise ValueError("Spark session must be provided for writing to abfss:// paths.")
        rdd = spark.sparkContext.parallelize([text]) if isinstance(text, str) else spark.sparkContext.parallelize(text)
        df = rdd.toDF(["value"])
        df.write.mode("overwrite").text(path)
    else:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

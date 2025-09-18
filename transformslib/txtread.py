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

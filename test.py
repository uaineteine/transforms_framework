if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql import DataFrame
    from transforms_class import DropVariable
    from metaplus_table import MetaplusTable

    # Create Spark session
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # Example DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)

    # Mock MetaplusTable class if not available
    class MetaplusTable:
        def __init__(self, df: DataFrame, table_name: str):
            self.df = df
            self.table_name = table_name

    # Wrap DataFrame in MetaplusTable
    tbl = MetaplusTable(df, "people")

    # Instantiate DropVariable transform
    drop_age = DropVariable(name="drop_age", description="Drops the age column", variable_to_drop="age")

    # Apply transform
    result_df = drop_age(tbl)

    # Show result
    print("Original columns:", df.columns)
    print("Transformed columns:", result_df.columns)
    result_df.show()
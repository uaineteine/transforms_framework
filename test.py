if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql import DataFrame
    from transforms_class import DropVariable
    from metaframe import Metaframe

    # Create Spark session
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # Wrap DataFrame in Metaframe
    tbl = Metaframe.load(spark=spark, path="test.csv", format="csv", table_name="test_table", frame_type="pyspark")

    # Instantiate DropVariable transform
    tbl = DropVariable("age")(tbl)

    # Show result
    print("Original columns:", df.columns)
    print("Transformed columns:", tbl.df.columns)
    tbl.df.show()
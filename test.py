if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from transforms_class import DropVariable
    from metaframe import Metaframe

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # Wrap DataFrame in Metaframe
    mf = Metaframe.load(spark=spark, path="test.csv", format="csv", table_name="test_table", frame_type="pyspark")
    print("Original columns:", tbl.df.columns)

    # Instantiate DropVariable transform
    mf = DropVariable("age")(tbl)

    # Show result
    print("Transformed columns:", tbl.df.columns)
    mf.df.show()


    #save table events
    mf.save_events()
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from transforms import DropVariable
    from pipeline_table import PipelineTable
    from supply_load import SupplyLoad

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # load pipeline table
    mf = PipelineTable.load(spark=spark, path="test_tables/test.csv", format="csv", table_name="test_table", frame_type="pyspark")
    print("Original columns:", mf.columns)

    # Instantiate DropVariable transform
    mf = DropVariable("age")(mf)

    # Show result
    print("Transformed columns:", mf.columns)
    mf.df.show()


    #save table events
    mf.save_events()


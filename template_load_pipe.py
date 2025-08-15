if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from transforms import DropVariable
    from supply_load import SupplyLoad

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    # load pipeline tables
    supply_frames = SupplyLoad("test_tables/payload.json", spark=spark)
    print("Original columns:", supply_frames["test_table"].columns)

    # Instantiate DropVariable transform
    supply_frames["test_table"] = DropVariable("age")(supply_frames["test_table"])

    # Show result
    print("Transformed columns:", supply_frames["test_table"].columns)
    supply_frames["test_table"].df.show()


    #save table events
    supply_frames.save_events()
    

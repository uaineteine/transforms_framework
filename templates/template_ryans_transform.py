if __name__ == "__main__":
    import os
    import sys
    
    os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"

    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Add the parent directory to sys.path
    parent_dir = os.path.join(current_dir, '..')
    sys.path.append(os.path.abspath(parent_dir))

    #---TEMPLATE STARTS HERE---
    from pyspark.sql import SparkSession
    from transformslib.transforms.atomiclib import *
    from pyspark.sql.functions import col
    from transformslib.tables.collections.supply_load import SupplyLoad
    from transformslib.transforms.dataset_transforms_base import *
    from transformslib.transforms.dataset_transforms import *

    # Create Spark session
    print("Creating Spark session")
    spark = SparkSession.builder.master("local").appName("TransformTest").getOrCreate()

    locations_metaframe = MetaFrame.load(
        "./test_tables/location.csv",
        "csv",
        "locations",
        spark=spark
    )

    positions_metaframe = MetaFrame.load(
        "./test_tables/positions.csv",
        "csv",
        "positions",
        spark=spark
    )

    table_collection = TableCollection([locations_metaframe, positions_metaframe])
    dataset_transforms = DatasetTransforms(table_collection)
    
    # dataset_transforms.add_transform(TopBottomCodeTransform("locations", "income", 0, 100000))
    dataset_transforms.add_transform(SHA256HashTransform("locations", "name"))
    # dataset_transforms.add_transform(JoinTransform("dataframe1", "dataframe2", "merged_dataframe", how="outer"))

    output_tables = dataset_transforms.get_output_tables()
    print(output_tables)

    print("DONE")
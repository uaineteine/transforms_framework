test_str = "this is a test file"
path = "test_tables/unit_test_subpackage/this_is_a_test.txt"

from pyspark.sql import SparkSession
from adaptiveio import save_raw_text, read_raw_text

sparkSession = SparkSession.builder.master("local").appName("adio").getOrCreate()

save_raw_text(path, test_str, spark=sparkSession)

final_str = read_raw_text(path, spark=sparkSession)

if test_str == final_str:
    print("TEST WORKING")
else:
    print(f"Input string: {test_str}")
    print(f"Output string: {final_str}")
    raise ValueError("The input and output string was supposed to match in this test")

import os
import sys
import time

from pyspark.sql import SparkSession

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Add the parent directory to sys.path
parent_dir = os.path.join(current_dir, '../..')
sys.path.append(os.path.abspath(parent_dir))
#---TEMPLATE STARTS HERE---

if __name__ == "__main__":
    # For Windows, set HADOOP_HOME to use winutils BEFORE importing Spark
    is_windows = sys.platform.startswith('win')
    if is_windows:
        # Point to the hadoop directory in the project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        hadoop_home = os.path.join(project_root, "hadoop")
        hadoop_bin = os.path.join(hadoop_home, "bin")
        os.environ["HADOOP_HOME"] = hadoop_home
        # Add hadoop\bin to PATH so Java can find hadoop.dll
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
        # Disable Hadoop native library warnings
        os.environ["HADOOP_OPTS"] = "-Djava.library.path="
    
    #start recording run time
    start_time = time.time()
    print(f"Starting test pipeline execution at {time.ctime(start_time)}")

    # Create Spark session
    print("Creating Spark session")
    appName = "TransformTest"
    # Set driver memory before creating the Spark session
    spark = SparkSession.builder\
            .master("local")\
            .appName(appName)\
            .config("spark.driver.memory", "2g")\
            .config("spark.hadoop.fs.permissions.umask-mode", "000")\
            .getOrCreate()
    # Access Hadoop configuration properly
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    hadoop_conf.set("parquet.summary.metadata.level", "NONE")
    
    from transformslib.engine import set_engine, set_spark_session
    set_engine("pyspark")
    set_spark_session(spark)

    from transformslib.tables.collections.resources import *

    print("TEST LOADING ENTITY MAP FOR ID GROUP 1")

    mf = load_ent_map([1])
    mf.show()

    print("TEST LOADING ENTITY MAP FOR ID GROUPS 1 and 2")

    mf = load_ent_map([2,1])
    mf.show()

    print("TEST COMPLETE")

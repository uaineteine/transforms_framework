# Engine module to set the execution spark sessions and framework for data operations

import os
from pyspark.sql import SparkSession

# Global variables inside this module
_sparkSession = None

def get_spark() -> SparkSession:
    return _sparkSession

def get_engine() -> str:
    return os.getenv("TNSFRMS_EXE_ENGINE", "polars")

def set_engine(new_engine:str):
    if new_engine == "":
        raise ValueError("EX0001 execution engine cannot be blank")
    #else
    os.environ["TNSFRMS_EXE_ENGINE"] = new_engine

def set_spark_session(spark=None):
    """
    Set or initialize the global Spark session and processing engine.
    
    Args:
        spark (SparkSession, optional): An existing SparkSession to use.
                                        If None, tries to create one.
    
    Returns:
        tuple: (sparkSession, processing_engine)
    """
    
    global _sparkSession
    processing_engine = get_engine()
    
    print(f"Current setup engine is: {processing_engine}")

    if spark is not None:
        # Use the provided SparkSession
        _sparkSession = spark
        processing_engine = "pyspark"
        print("Using provided Spark session. Engine set to pyspark.")
    else:
        try:
            # Try to create or get an existing SparkSession
            _sparkSession = SparkSession.builder.getOrCreate()
            processing_engine = "pyspark"
            print("Spark session initialized. Engine set to pyspark.")
        except Exception:
            # If Spark is not available, fallback
            _sparkSession = None
            processing_engine = "polars"
            print("No Spark session available. Defaulting to polars engine.")

    set_engine(processing_engine)

    return processing_engine

def detect_if_dbutils_available() -> bool:
    """Returns if dbutils is available in the current py engine"""
    #test if dbutils is available
    try:
        dbls = dbutils.fs.ls("/")
        return True
    except NameError:
        print("dbutils is NOT available")
        return False
    except Exception as e:
        print(f"SL112 failed checking for dbutils: {e}")
        return False
    
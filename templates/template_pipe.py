import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#tmp for test data
from pyspark.sql.functions import to_date

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Add the parent directory to sys.path
parent_dir = os.path.join(current_dir, '..')
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

    from transformslib.tables.collections.supply_load import SupplyLoad, clear_last_run
    from transformslib.transforms.atomiclib import *
    from transformslib.transforms.macrolib import *
    from transformslib import set_job_id, set_default_variables

    set_default_variables()

    # load pipeline tables
    job_id = 1
    run_id = 1
    set_job_id(job_id, new_run_id=run_id, mode="prod")

    clear_last_run()

    ent_keys = {"name":3, "id":2}

    supply_frames = SupplyLoad(ent_keys=ent_keys) #sample_rows=xyz
    
    listatomic()

    listmacro()
    
    # -------------------------------
    # Test Resources: Try and rad the resource files
    # -------------------------------
    supply_frames["entity_map"].show()

    # -------------------------------
    # Test NOVA GROUP: clone, create col transforms
    # -------------------------------
    print("Cloning a column.. Pre-transformation:")
    supply_frames["salary"].show()

    clone = DuplicateColumn("salary", "salary_clone")
    supply_frames = clone.apply(supply_frames, df="salary")

    #create new column of initial value of 5
    new_col = CreateColumn("new_column_of_five", 5)
    supply_frames = new_col.apply(supply_frames, df="salary")

    print("we should expect a cloned column and a new column of 5s")
    supply_frames["salary"].show()

    # -------------------------------
    # Test NOVA GROUP 2: arithmetic operations
    # -------------------------------

    print("adding new values together in salary table to give us 10")
    supply_frames = Arithmetic("new_column_of_five", "+", "new_column_of_five", "new_column_of_ten").apply(supply_frames, df="salary")

    print("subtracting new values should give us 0")
    supply_frames = Arithmetic("new_column_of_ten", "-", "new_column_of_ten", "new_column_of_zero").apply(supply_frames, df="salary")

    print("multiply new values should give us 50")
    supply_frames = Arithmetic("new_column_of_five", "*", "new_column_of_ten", "new_column_of_fifty").apply(supply_frames, df="salary")

    print("exponent of new values should give us 3125")
    supply_frames = Arithmetic("new_column_of_five", "**", "new_column_of_five", "new_exponent_result").apply(supply_frames, df="salary")

    print("divide of new values should give us 625")
    supply_frames = Arithmetic("new_exponent_result", "/", "new_column_of_five", "new_divide_result").apply(supply_frames, df="salary")

    supply_frames["salary"].show()


    # -------------------------------
    # Test 1: PartitionByValue on SALARY for salary
    # -------------------------------
    print("Partitioning salary by salary")
    partition_transform = PartitionByValue("salary")
    supply_frames = partition_transform.apply(supply_frames, df="salary")
    partitioned = supply_frames.select_by_names("salary_*")
    for t in partitioned.tables:
        print(f"Table: {t.table_name}")
        t.show()

    # -------------------------------
    # Test 2: DropVariable on positions
    # -------------------------------
    print("Original columns (positions):", supply_frames["positions"].columns)

    supply_frames = DropVariable("var").apply(supply_frames, df="positions")

    print("After DropVariable (var) on positions:", supply_frames["positions"].columns)
    supply_frames["positions"].show()

    # -------------------------------
    # Test 3: SubsetTable on salary
    # -------------------------------
    print("Original columns (salary):", supply_frames["salary"].columns)

    supply_frames = SubsetTable(["salary", "age"]).apply(supply_frames, df="salary")

    print("After SubsetTable (keep salary) on salary:", supply_frames["salary"].columns)
    supply_frames["salary"].show()

    # -------------------------------
    # Test 4: DistinctTable on positions
    # -------------------------------
    print("Applying DistinctTable on positions")
    supply_frames = DistinctTable().apply(supply_frames, df="positions")

    print("After DistinctTable on positions (all columns):")
    supply_frames["positions"].show()

    # -------------------------------
    # Test 5: DistinctTable on salary
    # -------------------------------
    print("Applying DistinctTable on salary")
    supply_frames = DistinctTable().apply(supply_frames, df="salary")

    print("After DistinctTable on salary (all columns):")
    supply_frames["salary"].show()

    # -------------------------------
    # Test 6: RenameTable on salary
    # -------------------------------
    print("Original columns (salary):", supply_frames["salary"].columns)

    supply_frames = RenameTable({"salary": "income"}).apply(supply_frames, df="salary")

    print("After RenameTable (salary -> income) on salary:", supply_frames["salary"].columns)
    supply_frames["salary"].show()

    # -------------------------------
    # Test 7: ComplexFilter on salary
    # -------------------------------
    print("Applying ComplexFilter (income >= 600) on salary")
    
    #TODO 
    #make a simple filter type vs a complex one

    filter_transform = ComplexFilter(condition_map={
        "pyspark": lambda df: df.filter(col("income") >= 600)
    })

    supply_frames = filter_transform.apply(supply_frames, df="salary")

    print("After ComplexFilter (income > 600) on salary:")
    supply_frames["salary"].show()

    # -------------------------------
    # Test 8: JoinTable on positions and salary
    # -------------------------------
    print("Joining positions and salary on age")

    join_transform = JoinTable(
        left_table="positions",
        right_table="salary",
        join_columns="age",
        join_type="inner"
    )

    supply_frames = join_transform.apply(supply_frames, output_table="example_join")

    print("After JoinTable (positions inner join salary on age):")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 9: SimpleFilter on the joined table
    # -------------------------------
    print("Applying SimpleFilter (income > 600) on example_join")
    supply_frames = SimpleFilter(column="income", op=">", value=600).apply(supply_frames, df="example_join")

    print("After SimpleFilter:")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 10: Concatenate variables
    # -------------------------------
    print("Concatenating variables")
    supply_frames = ConcatColumns(variables_to_concat=["age", "skill"], sep="_").apply(supply_frames, df="example_join", output_var="concatted")
    print("After ConcatColumns (age, skill -> concatted) on example_join:")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 11: ReplaceByCondition
    # -------------------------------
    print("Replacing values in income where income >= 610 with 600")
    supply_frames = ReplaceByCondition(
        column="income",
        op=">=",
        value=610,
        replacement=600
    ).apply(supply_frames, df="example_join")

    print("After ReplaceByCondition (income >= 610 -> 600):")
    supply_frames["example_join"].show()

    # -------------------------------
    # Test 12: explode some table
    # -------------------------------
    
    print("Exploding an array type test")
    supply_frames = ExplodeColumn("var3", " ", True).apply(supply_frames, df="array_like")
    supply_frames["array_like"].show()

    # -------------------------------
    # Test 13: RoundNumber
    # -------------------------------
    print("Rounding a number to 3 decimal places")
    supply_frames = RoundNumber("value2", 3).apply(supply_frames, df="decimal_table")
    supply_frames["decimal_table"].show()

    # -------------------------------
    # Test 14: TruncateDate
    # -------------------------------
    print("Truncating a date to year and month levels")
    # Convert string to date
    supply_frames["date_table"].df = supply_frames["date_table"].df.withColumn("event_date", to_date("event_date", "yyyy-MM-dd"))
    
    supply_frames = TruncateDate("event_date", "month").apply(supply_frames, df="date_table")
    supply_frames["date_table"].show()
    supply_frames = TruncateDate("event_date", "year").apply(supply_frames, df="date_table")
    supply_frames["date_table"].show()
    #print(supply_frames["date_table"].dtypes)

    # -------------------------------
    # Test 15: Sorting
    # -------------------------------
    print("Sorting the date_table by event_date")
    supply_frames = SortTable(by="event_date", ascending=True).apply(supply_frames, df="date_table")
    supply_frames["date_table"].show()

    # -------------------------------
    # Test 16: ForceCase
    # -------------------------------
    print("Applying ForceCase (upper) on var2 column")
    supply_frames = ForceCase("var2", "upper").apply(supply_frames, df="array_like")
    print("After ForceCase (upper):")
    supply_frames["array_like"].show()

    # -------------------------------
    # Test 17: TrimWhitespace
    # -------------------------------
    print("Applying TrimWhitespace on name column")
    supply_frames = TrimWhitespace("name").apply(supply_frames, df="location")
    print("After TrimWhitespace:")
    supply_frames["location"].show()

    # -------------------------------
    # Test 18: TopBottomCoding
    # -------------------------------
    print("Applying TopBottomCode macro to salary column")
    s_col = supply_frames.select_by_names("salary")
    s_col = TopBottomCode(s_col, ["income"], 500, 450).apply()
    print("Original salary data:")
    supply_frames["salary"].show()
    
    # -------------------------------
    # Test 30: Union Tables
    # ------------------------
    print("Applying UnionTables on two salary tables (with and without duplicates)")
    union_transform = UnionTables("salary_400", "salary_500", union_all=False)
    supply_frames = union_transform.apply(supply_frames, output_table="salary_union")
    print("After UnionTables (salary_400 and salary_500):")
    supply_frames["salary_union"].show()
    
    # -------------------------------
    # Test 21: Attach synthetic ID test
    # -------------------------------
    atn_synth = AttachSynID("name", ignore_tests=True)
    supply_frames = atn_synth.apply(supply_frames, df="location")
    supply_frames["location"].show()

    # -------------------------------
    # Test 19: HASHING
    # -------------------------------
    print("Applying hashing test")
    hsh = HashColumns("region", "hextest")
    supply_frames = hsh.apply(supply_frames, df="location")
    supply_frames["location"].show()
    
    # -------------------------------
    # Test 20: HMAC HASHING - Atomic version
    # -------------------------------
    
    #print("Applying HMAC hashing on city column with key length 24")
    #hmac = ApplyHMAC("city", 24)
    #supply_frames = hmac.apply(supply_frames, df="location", hmac_key="a super secret key")
    #supply_frames["location"].show()

    # -------------------------------
    # Apply TopBottomCode macro to salary table
    # -------------------------------
    print("Setting minimum value to 450 and maximum value to 650")
    
    # Create TopBottomCode macro instance
    topbottom_macro = TopBottomCode(
        input_tables=s_col,
        input_variables=["income"],  # Apply coding to salary column
        max_value=650,               # Cap values above 650
        min_value=450                # Floor values below 450
    )
    
    # Apply the macro transformation to the salary table
    s_col = topbottom_macro.apply()
    
    print("After TopBottomCode transformation:")
    supply_frames["salary"].show()
    
    # -------------------------------
    # Test 22: MetaFrame Metadata Tracking
    # -------------------------------
    print("Testing MetaFrame metadata tracking (warning_messages and person_keys)")
    
    # Display formatted info for salary table
    print("\nFormatted info for 'salary' table:")
    supply_frames["salary"].info()
    
    # Display formatted info for location table
    print("\nFormatted info for 'location' table:")
    supply_frames["location"].info()
    
    # Print info for all tables using TableCollection
    print("-------------------------------")
    print("\nInfo for all tables via TableCollection.get_info():")
    supply_frames.get_info()

    # -------------------------------
    # Test 23: DropNAValues
    # -------------------------------
    print("Applying DropNAValues on salary table")
    print("Before DropNAValues on age column in positions table:")
    supply_frames["positions"].show()

    drop_nulls = DropNAValues("age")
    supply_frames = drop_nulls.apply(supply_frames, df="positions")
    
    print("After DropNAValues on age column in positions table:")
    supply_frames["positions"].show()
    
    # -------------------------------
    # Repeated tests to continue
    # -------------------------------

    print("\nexample of final table going through some tests")
    join_transform = JoinTable(
        left_table="location",
        right_table="state",
        join_columns="city",
        join_type="outer"
    )
    print("first join complete")
    supply_frames = join_transform.apply(supply_frames, output_table="location")
    supply_frames = DistinctTable().apply(supply_frames, df="location")
    
    supply_frames["location"].show()

    # save table output tables

    #keep only salary tables
    supply_frames.select_by_prefix("salary*").save_all()
    print("Subset of tables with prefix 'salary' saved successfully")

    end_time = time.time()
    print(f"Test pipeline execution completed at {time.ctime(end_time)}")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    
    # Properly stop Spark to ensure clean exit
    spark.stop()
    print("Spark session stopped successfully")
    
    # Explicit successful exit
    sys.exit(0)
    
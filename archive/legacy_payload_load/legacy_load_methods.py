def legacy_get_payload_file(job_id: int, run_id: int, use_test_path: bool = False) -> str:
    """
    Return the path location of the legacy input payload (payload.json).

    Args:
        job_id (int): A job id to get the path of configuration file containing supply definitions.
        run_id (int): A run id to get the path of configuration file containing supply definitions.
        use_test_path (bool, optional): Whether to use the local test path (LOCAL_TEST_PATH) or the production path (WORM_PATH). Defaults to False (use production path).

    Returns:
        str: The legacy payload path.
    """
    base_path = LOCAL_TEST_PATH if use_test_path else WORM_PATH
    print(f"[LEGACY] Using legacy payload method for job_id={job_id}, run_id={run_id}")
    return f"{base_path}/job_{job_id}/payload.json"


def load_from_payload(data: Dict[str, Any], tables: list, named_tables: Dict[str, Any], 
                     sample: bool, sample_rows: int = None, sample_frac: float = None, 
                     seed: int = None, spark=None) -> None:
    """
    Load supplies from legacy payload.json format.

    Args:
        data (Dict[str, Any]): The parsed JSON data from payload.json
        tables (list): List to append loaded tables to
        named_tables (Dict[str, Any]): Dictionary to store named table references
        sample (bool): Whether to apply sampling to loaded tables
        sample_rows (int, optional): Number of rows to sample
        sample_frac (float, optional): Fraction of rows to sample
        seed (int, optional): Random seed for reproducible sampling
        spark: SparkSession object for PySpark operations

    Returns:
        None
    """
    print("Loading supplies from legacy payload.json format")
    supply = data.get("supply", [])
    
    for item in supply:
        name = item.get("name")
        if not name:
            raise ValueError("Each supply item must have a 'name' field")

        print(f"Loading table '{name}' from {item['path']} (format: {item['format']})")

        table = MetaFrame.load(
            path=item["path"],
            format=item["format"],
            frame_type="pyspark",
            spark=spark
        )

        # If CSV, cast columns to expected dtypes if provided
        if item["format"].lower() == "csv" and "dtypes" in item:
            print(f"Casting columns for table '{name}' to expected schema...")
            dtypes = item["dtypes"]
            # Build a dict of column: pyspark type string
            pyspark_type_map = {
                "String": "string",
                "Int64": "long",
                "Float64": "double",
                "Boolean": "boolean"
            }
            from pyspark.sql.functions import col
            for colname, dtypeinfo in dtypes.items():
                target_type = dtypeinfo.get("dtype_output") or dtypeinfo.get("dtype_source")
                if target_type in pyspark_type_map:
                    spark_type = pyspark_type_map[target_type]
                    try:
                        table.df = table.df.withColumn(colname, col(colname).cast(spark_type))
                    except Exception as e:
                        print(f"Warning: Could not cast column '{colname}' to {spark_type}: {e}")
                else:
                    print(f"Warning: Unknown dtype '{target_type}' for column '{colname}'")

        # Apply sampling if requested
        if sample:
            table.sample(n=sample_rows, frac=sample_frac, seed=seed)

        tables.append(table)
        named_tables[name] = table

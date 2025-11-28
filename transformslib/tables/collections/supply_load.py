import os
from typing import Dict, Any
from transformslib.engine import get_engine, get_spark
from tabulate import tabulate
from adaptiveio import load_json
from multitable import MultiTable, SchemaValidator
from transformslib.tables.metaframe import MetaFrame
from transformslib.transforms.reader import transform_log_loc, does_transform_log_exist
from .collection import TableCollection
from transformslib.templates.pathing import apply_formats

def get_schema_summary(expected_dtypes: Dict[str, Dict[str, str]]) -> str:
    """
    Get a human-readable summary of the expected schema.
    
    Args:
        expected_dtypes: Dictionary mapping column names to dtype information
        
    Returns:
        str: A formatted string describing the expected schema
    """
    summary_lines = ["Expected Schema:"]
    for col_name, dtype_info in expected_dtypes.items():
        dtype_source = dtype_info.get('dtype_source', 'Unknown')
        dtype_output = dtype_info.get('dtype_output', dtype_source)
        summary_lines.append(f"  {col_name}: {dtype_source} -> {dtype_output}")
    
    return "\n".join(summary_lines)

class SchemaValidationError(Exception):
    """Exception raised when schema validation fails."""
    pass

def get_execution_engine_info() -> Dict[str, Any]:
    """
    Get databricks information if running in databricks environment
    """
    ALL_VARS = os.environ

    #append with dbutils notebook info
    try:
        path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        ALL_VARS["DATABRICKS_NOTEBOOK_PATH"] = path
        ALL_VARS["DATABRICKS_NOTEBOOK_NAME"] = os.path.basename(path)
        ALL_VARS["DATABRICKS_WORKSPACE_URL"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
        ALL_VARS["DATABRICKS_CLUSTER_ID"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("clusterId").get()
        ALL_VARS["DATABRICKS_USER"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
        
    except NameError:
        pass
    except Exception as e:
        print(f"SL020 Warning: Unhandled exception to get databricks notebook path: {e}")

    return ALL_VARS

def clear_last_run():
    """
    Remove output paths on request
    """
    if does_transform_log_exist():
        #remove it using 2 different systems
        path = transform_log_loc()
        if "dbfs:/" in path:
            try:
                #test if dbutils is available
                dbls = dbutils.fs.ls("/")

                dbutils.fs.rm(path, True)
                print("Cleared the transform log path using dbutils")
            except NameError:
                print("SL012 dbutils is NOT available to clear the path")
        else:
            os.remove(path)

def get_run_state() -> str:
    """
    Return the path location of the input payload.

    Returns:
        str: The payload path.
    """
    path = os.environ.get("TNSFRMS_JOB_STATE", "../test_tables")
    path = apply_formats(path)

    return path

def get_supply_file(table_name: str) -> str:
    """
    Return the path location of the input payload.

    Args: table_name (str): The name of the table to load.

    Returns:
        str: The payload path.
    """
    base_path = os.environ.get("TNSFRMS_JOB_PATH", "../test_tables")
    #format the path for job_id
    path = apply_formats(base_path)
    path = path.replace("{tablename}", table_name)
    
    print(path)
    
    return path

def get_table_names_from_run_state(run_state: Dict[str, Any]) -> list[str]:
    """
    Extract a unique, sorted list of table_name values from a run_state dict
    (looks under all_files -> data_files, map_files, enum_file, schema_file, other_files).
    
    Input directory is the run_state.json
    """
    names = set()
    all_files = run_state.get("all_files", {})
    for entry in all_files.get("data_files", []):
            tn = entry.get("table_name")
            if tn:
                names.add(tn)
    return sorted(names)

def load_pre_transform_data(spark=None) -> list[MultiTable]:
    """
    Load the pre-transform tables for supply loading.
    
    Returns Multitable lists of frames
    """
    
    colpath = os.environ.get("TNSFRMS_JOB_COLS_PATH", "../test_tables/jobs/{prodtest}/{job_id}/run/{run_id}/data_quality/pre_transform_columns.delta")
    sumpath = os.environ.get("TNSFRMS_TABLE_SUMMARY_PATH", "../test_tables/jobs/{prodtest}/{job_id}/run/{run_id}/data_quality/pre_transform_table_summary.delta")
    
    def _load_table(path:str, format=None, spark=None) -> MultiTable:
        """
        Load a pre transform table from the given path. Applying formats if needed and attempting de-duplication.
        
        Args:
            path (str): The path to the delta table.
            format (str): The format of the table, default is "delta".
            spark: SparkSession object for PySpark operations.

        Returns:
            MultiTable: The loaded MultiTable instance.
        """
        path = apply_formats(path)

        if format == None:
            #infer the format from the path
            format = path.split(".")[-1]
            if format.lower() not in ["parquet", "delta", "csv", "json"]:
                format = "delta"
        else:
            if len(format) == 0:
                raise ValueError("MT013 Format string cannot be empty")
        
        #lowercase override
        format = format.lower()
        try:
            if (spark is None):
                mt = MultiTable.load(
                    path=path,
                    format=format,
                    frame_type="pandas"
                )
            else:
                mt = MultiTable.load(
                    path=path,
                    format=format,
                    frame_type="pyspark",
                    spark=spark
                )
        except Exception as e:
            print(f"SL050 Error loading pre-transform table at {path}: {e}")
            raise e
        
        try:
            mt = mt.distinct()
        except Exception as e:
            print(f"SL011 Error processing newly loaded pre-transform tables: {e}")
            raise e

        return mt

    col_df = _load_table(colpath, spark=spark)
    sum_df = _load_table(sumpath, spark=spark)
    
    #deuplicate frames before returning
    return col_df, sum_df

def load_single_table_from_sampling(data: Dict[str, Any],
        sample: bool, sample_rows: int = None, sample_frac: float = None,
        seed: int = None, spark=None, enable_schema_validation: bool = True) -> MetaFrame:
    """
    Load supplies from new sampling_state.json format with optional schema validation.
    
    Args:
        data (Dict[str, Any]): The parsed JSON data from sampling_state.json
        sample (bool): Whether to apply sampling to loaded tables
        sample_rows (int, optional): Number of rows to sample
        sample_frac (float, optional): Fraction of rows to sample
        seed (int, optional): Random seed for reproducible sampling
        spark: SparkSession object for PySpark operations
        enable_schema_validation (bool): Whether to perform schema validation. Defaults to True.

    Returns:
        MetaFrame: The loaded MetaFrame instance.
    """
    name = data.get("table_name")
    if not name:
        raise ValueError("Each sample file item must have a 'table_name' field")

    # Handle path normalization - sampling_state.json may use relative paths
    # Some older versions use input_file_path, sticking to that for backward compatibility
    file_path = data.get("input_path", "NONE") #default to NONE if not found
    if file_path == "NONE" and "input_file_path" in data:
        print("SL030 Warning: 'input_file_path' is deprecated, please use 'input_path' instead on source")
        file_path = data["input_file_path"]
        

    print(f"Loading table '{name}' from {file_path} (format: {data['file_format']})")

    #error checking the file format
    fmt = data["file_format"].lower()
    if fmt not in ["part_parquet", "parquet", "csv", "json", "delta"]:
        raise ValueError(f"SL031 Unsupported file format '{data['file_format']}' for table '{name}'")
    #patching file format for read if needed
    if fmt == "part_parquet":
        fmt = "parquet"
    table = MetaFrame.load(
            path=file_path,
            format=fmt,
            frame_type="pyspark",
            spark=spark
    )

    # Perform schema validation if enabled and dtypes are provided
    if enable_schema_validation and "dtypes" in data:
        try:
            print(f"Validating schema for table '{name}'...")
            dtypes = data["dtypes"]
            # Print schema summary for transparency
            schema_summary = get_schema_summary(dtypes)
            print(schema_summary)
            # Validate the schema
            SchemaValidator.validate_schema(
                df=table.df,
                expected_dtypes=dtypes,
                frame_type=table.frame_type,
                table_name=name
            )
            print(f"Schema validation passed for table '{name}'")
        except SchemaValidationError as e:
            print(f"SL007 Schema validation failed for table '{name}': {e}")
            raise e
        except Exception as e:
            print(f"Warning: SL006 Unexpected error during schema validation for table '{name}': {e}")
    elif enable_schema_validation:
        print(f"Warning: No schema information (dtypes) found for table '{name}' - skipping validation")

    # Apply sampling if requested
    if sample:
        table.sample(n=sample_rows, frac=sample_frac, seed=seed)

    return table

class SupplyLoad(TableCollection):
    """
    A specialised collection manager for loading and managing supply data from JSON configuration files.

    This class extends TableCollection to provide automated loading of multiple data sources from a JSON configuration file. It supports thr sampling input method using sampling_state.json (requires only job_id)
    
    The new sampling system includes schema validation capabilities that automatically verify
    loaded data against the expected schema defined in the dtypes field.
    
    New sampling state JSON configuration follows this structure:
    {
        "sample_files": [
            {
                "table_name": "table_name",
                "input_path": "path/to/data.parquet",
                "file_format": "parquet",
                "dtypes": {
                    "column1": {"dtype_source": "String", "dtype_output": "String"},
                    "column2": {"dtype_source": "Int64", "dtype_output": "Int64"}
                }
            ]
        }

    Attributes:
        supply_load_src (str): The path to the JSON configuration file.
        job (int): The job ID for the current operation.
        run (int): The run ID for the current operation (None for new sampling input method).
        enable_schema_validation (bool): Whether schema validation is enabled (new system only).
    
    Example:
        >>> # New sampling input method with schema validation (default)
        >>> supply_loader = SupplyLoad(job_id=1, spark=spark)
        >>> 
        >>> # New sampling input method without schema validation
        >>> supply_loader = SupplyLoad(job_id=1, spark=spark, enable_schema_validation=False)

        >>> customers_table = supply_loader["customers"]
        >>> orders_table = supply_loader["orders"]

        >>> # Save events for all loaded tables
        >>> supply_loader.save_events()
    """
    
    def __init__(self, sample_frac: float = None, sample_rows: int = None, seed: int = None, enable_schema_validation: bool = True):
        """
        Initialise a SupplyLoad instance with a JSON configuration file.

        This constructor loads the JSON configuration file and automatically creates
        MetaFrame instances for each supply item defined in the configuration.
        All tables are loaded as PySpark DataFrames by default.

        Args:
            sample_frac (float, optional): Fraction of rows to sample (0 < frac <= 1).
            sample_rows (int, optional): Number of rows to sample.
            seed (int, optional): Random seed for reproducibility.
            enable_schema_validation (bool, optional): Enable schema validation for new sampling system. 
                                                     Only applies when run_id is None (new system).
                                                     Defaults to True.

        Raises:
            FileNotFoundError: If the JSON configuration file doesn't exist.
            ValueError: If the output transform file already exists suggesting the run has been made before.
            SchemaValidationError: If schema validation fails (only for new sampling system).
            Exception: If there are issues loading any of the data files.

        Example:

            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.appName("SupplyLoad").getOrCreate()
            >>> 
            >>> # New sampling input method with schema validation (default)
            >>> supply_loader = SupplyLoad(job_id=1, spark=spark)
            >>> 
            >>> # New sampling input method without schema validation
            >>> supply_loader = SupplyLoad(job_id=1, spark=spark, enable_schema_validation=False)

            >>> print(f"Loaded {len(supply_loader)} tables")
        """
        
        # Initialise the parent class with empty tables list
        super().__init__(tables=[])

        #run parameters
        self.job = os.environ.get("TNSFRMS_JOB_ID", 1)
        self.run = os.environ.get("TNSFRMS_RUN_ID", 1) 
        self.enable_schema_validation = enable_schema_validation

        self.supply_load_src = get_run_state()
        
        #gather the source payload location
        self.output_loc = transform_log_loc()
        if (does_transform_log_exist()):
            raise ValueError("SL010 Transform has been run beforehand, please CLEAR previous result or use new run id")

        if sample_frac != None or sample_rows != None:
            self.sample = True
            self.sample_frac = sample_frac
            self.sample_rows = sample_rows
            self.seed = seed
        else:
            self.sample = False
            self.sample_frac = None
            self.sample_rows = None
            self.seed = seed

        names_of_loaded = self.load_supplies()

    def load_supplies(self) -> list[str]:
        """
        Load supply data from the JSON configuration file.

        This method reads either a sampling_state.json (new sampling input method)
        configuration file and creates MetaFrame instances for each supply item. It validates that each 
        supply item has the required fields, loads the data using the specified format and path, and 
        optionally applies sampling. For the new sampling system, schema validation is performed if enabled.
            
        Returns:
            List[str]: A list of names of the loaded tables.

        Raises:
            FileNotFoundError: If the JSON configuration file doesn't exist.
            ValueError: If the JSON format is invalid or missing required fields.
            SchemaValidationError: If schema validation fails (only for new sampling system).
            Exception: If there are issues loading any of the data files.

        Example:

            >>> supply_loader = SupplyLoad(job_id=1, spark=spark)  # New sampling input method
        """
        spark = None
        if get_engine() == "pyspark":
            spark = get_spark()

        table_names = []
        paths = []
        formats = []
        try:
            try:
                print(f"Reading the delta tables to extract meta information")
                col_df, sum_df = load_pre_transform_data(spark=spark)
            except FileNotFoundError:
                raise FileNotFoundError(f"SL003 Pre-transform delta tables not found for job {self.job} run {self.run}")
            
            paths_info = sum_df.copy()
            paths_info = paths_info.select("table_name", "table_path").distinct()
            paths_info = paths_info.sort("table_name")
            paths_info.show(truncate=False)
            
            #show column info
            col_info = col_df.select("table_name","column_name","description", "data_type", "warning_messages").distinct()
            col_info.show(truncate=False)

            try:
                #show warning messages - using pandas for easy display
                warnings_frame = col_df.select("table_name", "column_name", "warning_messages")
                #explode the warnings on pipe
                warnings_frame.explode("warning_messages", sep="|", outer=False)
                warnings_frame = warnings_frame.get_pandas_frame()
                warnings_frame = warnings_frame[warnings_frame["warning_messages"].notnull()]
                warnings_frame = warnings_frame.drop_duplicates()
                print(tabulate(warnings_frame, headers='keys', tablefmt='pretty', showindex=False))
            except Exception as e:
                print(f"SL009 Error in signposting: Could not extract warning messages: {e}")

            #show table names and convert to a list
            #collect the table names from the frame
            paths_info = paths_info.get_pandas_frame()
            print(tabulate(paths_info, tablefmt='pretty', showindex=False))
            table_names = paths_info["table_name"].tolist()
            
            paths = paths_info["table_path"]
            paths = paths.tolist()
            
            formats = paths_info["format"].tolist()
        
        except Exception as e:
            print(f"SL010 Error reading pre-transform delta tables: Exception {e}")
        
        if table_names == []:
            print("")
            print(f"Attempting supply loading from: {self.supply_load_src}")
            try:
                print("reading the state file")
                run_state = load_json(self.supply_load_src, spark=spark)
                table_names = get_table_names_from_run_state(run_state)
        
            except FileNotFoundError:
                raise FileNotFoundError(f"SL004 Supply JSON file not found at {self.supply_load_src}")
            except Exception as e:
                print(f"Error SL005 reading supply JSON file: {e}")
                raise e
        
        print(table_names)
        
        print("Transformslib will now attempt to load each table in the supply...")
       
        #using the json method given there is no path list
        if paths == []:
            print("")
            print("No paths found from pre-transform tables, using sampling input method")
            for t in table_names:
                try:
                    supply_file = get_supply_file(t)
                    print(f"Table '{t}' supply file located at: {supply_file}")
                    data = load_json(supply_file, spark=spark)

                    # Determine format based on the structure of the JSON file
                    if "table_name" in data:
                        print(f"Attempting load of table '{t}'")
                        # New sampling input method (sampling_state.json format)
                        # Schema validation is only available for the new system
                        mt = load_single_table_from_sampling(
                            data=data, 
                            sample=self.sample,
                            sample_rows=self.sample_rows,
                            sample_frac=self.sample_frac,
                            seed=self.seed,
                            spark=spark,
                            enable_schema_validation=self.enable_schema_validation
                        )

                        print(f"Load of table '{t}'")
                        self.tables.append(mt)
                        self.named_tables[t] = mt

                    else:
                        raise ValueError("SL002 Unrecognized JSON format: expected 'table_name' key")
                except Exception as e:
                    print(f"Error SL001 loading table '{t}': {e}")
                    raise e
        else:
            print("Using delta table method to load supplies...")
            print("")
            #flag error if lengths do not match
            if len(paths) != len(table_names):
                print("SL008")
                print("PATHS:")
                print(paths)
                print("TABLE NAMES:")
                print(table_names)
                
                raise ValueError("SL008 Mismatch in length between number of table names to load and data loaded paths")

            for i, t in enumerate(table_names):
                try:
                    mt = MetaFrame.load(
                        path=paths[i],
                        format=formats[i],
                        frame_type="pyspark",
                        spark=spark
                    )
                    self.tables.append(mt)
                    self.named_tables[t] = mt
                except Exception as e:
                    print(f"Error SL200 loading table '{t}' from {paths[i]}: {e}")
                    raise e

        print("")
        print(f"Successfully loaded {len(self.tables)} tables")
            
        print("Loaded the following tables: ")
        print(self.named_tables)
        
        return table_names

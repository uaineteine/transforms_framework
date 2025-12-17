import os
from typing import Dict, Any
from transformslib.engine import get_engine, get_spark, detect_if_dbutils_available
from tabulate import tabulate
from multitable import MultiTable, SchemaValidator
from transformslib.tables.metaframe import MetaFrame
from transformslib.transforms.reader import transform_log_loc, does_transform_log_exist
from .collection import TableCollection
from .resources import *
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
    dbav = detect_if_dbutils_available()
    if dbav:
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
            dbav = detect_if_dbutils_available()
            if dbav:
                dbutils.fs.rm(path, True)
                print("Cleared the transform log path using dbutils")
            else:
                raise NameError("SL012 dbutils could not clear the path")
        else:
            os.remove(path)

def load_input_table(path:str, format=None, spark=None) -> MultiTable:
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
            raise ValueError("SL013 Format string cannot be empty")
        
    engine = get_engine()
    mt = MultiTable.load(
        path=path,
        format=format,
        frame_type=engine,
        auto_lowercase=True,
        spark=spark
    )
    
    return mt

def load_summary_data(spark=None) -> MultiTable:
    """
    Load the pre-transform table summary data.
    
    Returns Multitable of frame
    """
    sumpath = os.environ.get("TNSFRMS_TABLE_SUMMARY_PATH", "../test_tables/jobs/{prodtest}/{job_id}/run/{run_id}/data_quality/pre_transform_table_summary.delta")
    sum_df = load_input_table(sumpath, spark=spark)
    
    #deuplicate frames before returning
    return sum_df

def load_column_data(spark=None) -> MultiTable:
    """
    Load the pre-transform column data.
    
    Returns Multitable of frame
    """
    colpath = os.environ.get("TNSFRMS_JOB_COLS_PATH", "../test_tables/jobs/{prodtest}/{job_id}/run/{run_id}/data_quality/pre_transform_columns.delta")
    col_df = load_input_table(colpath, spark=spark)
    
    #deuplicate frames before returning
    return col_df

def load_table_warnings(spark=None) -> pd.DataFrame:
    """
    Load and display warning messages from the pre-transform column data.
    """
    try:
        col_df = load_column_data(spark=spark)
        
        #show warning messages - using pandas for easy display
        warnings_frame = col_df.select("table_name", "column_name", "warning_messages")
        #explode the warnings on pipe
        warnings_frame.explode("warning_messages", sep="|", outer=False)
        warnings_frame = warnings_frame.get_pandas_frame()
        #turn the warning messages into lowercase strings
        warnings_frame["warning_messages"] = warnings_frame["warning_messages"].astype(str).str.lower()
        # Filter out NULL AND empty strings
        warnings_frame = warnings_frame[
            (warnings_frame["warning_messages"].notnull()) & 
            (warnings_frame["warning_messages"] != "")
        ]
        warnings_frame = warnings_frame.drop_duplicates()
        # Sort by table_name first, then column_name
        warnings_frame = warnings_frame.sort_values(by=["table_name", "column_name"])
        
        return warnings_frame
    except Exception as e:
        print(f"SL009 Error in warning messages: Could not extract warning messages: {e}")

def get_supply_srcs(spark=None) -> pd.DataFrame:
    """
    Load and return the supply sources from the pre-transform summary data.
    """
    sum_df = load_summary_data(spark=spark)

    #error flags
    if "format" not in sum_df.columns:
        raise ValueError("SL400 Summary data does not contain 'format' column")

    try:
        #filter down for target columns, sort by table name
        sum_df = sum_df.select("table_name", "table_path", "format").distinct()
        sum_df = sum_df.sort("table_name")
        sum_df = sum_df.get_pandas_frame()
        
        return sum_df
    except Exception as e:
        print(f"SL030 Error in extracting supply sources: {e}")

def gather_supply_ids(spark=None) -> list[int]:
    """
    Placeholder for parsing in data from a dataframe, will be replaced with integrated loading later.
    """
    sum_df = load_summary_data(spark=spark)
    
    #error checking
    if "id_group_cd" not in sum_df.columns:
        raise ValueError("SL950 ERROR there is no id_group_cd column in table summary data")
    
    #get distinct list of ids
    ids = []
    try:
        sum_df = sum_df.select("id_group_cd").distinct()
        #convert to pandas an extract the list
        sum_df = sum_df.get_pandas_frame()
        ids = sum_df["id_group_cd"].tolist()
    except Exception as e:
        print(f"SL951 {e}")
    
    return ids

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
    
    def __init__(self, sample_frac: float = None, sample_rows: int = None, seed: int = None, enable_schema_validation: bool = True, ent_keys:dict={}):
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
            ent_keys (dict): Dictionary of entity keys for loading the entity map.

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
        
        #gather the source payload location
        self.output_loc = transform_log_loc()
        if (does_transform_log_exist()):
            raise ValueError("SL021 Transform has been run beforehand, please CLEAR previous result or use new run id")

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

        self.load_supplies(ent_keys)

    def load_supplies(self, ent_keys:dict={}):
        """
        Load supply data from the JSON configuration file.

        This method reads either a sampling_state.json (new sampling input method)
        configuration file and creates MetaFrame instances for each supply item. It validates that each 
        supply item has the required fields, loads the data using the specified format and path, and 
        optionally applies sampling. For the new sampling system, schema validation is performed if enabled.
        
        Args:
            ent_keys (dict): Dictionary of entity keys for loading the entity map.
        
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

        print("Transformslib will now attempt to read in the table sources from the pre-transform summary data...")
        sources = get_supply_srcs(spark=spark)
        print("Transformslib has successfully read in the table sources.")
        print(tabulate(sources, tablefmt='pretty', showindex=False))
        
        print("Transformslib will now attempt to load each table in this supply...")
        
        engine = get_engine()
        for _, row in sources.iterrows():
            try:
                mt = MetaFrame.load(
                    path=row["paths"],
                    format=row["formats"],
                    frame_type=engine,
                    spark=spark
                )
                
                self.tables.append(mt)
                self.named_tables[row["table_name"]] = mt
                
                print(f"Successfully loaded table: {row['table_name']}")
            except Exception as e:
                print(f"SL200 Error loading {row['table_name']}: {e}")
            
        print("")
        print(f"Successfully loaded {len(self.tables)} tables")
        
        print("Transformslib will now gather the warning messages from the pre-transform column data...")
        try:
            #show warning messages - using pandas for easy display
            warnings_frame = load_table_warnings(spark=spark)
            print(tabulate(warnings_frame, headers='keys', tablefmt='pretty', showindex=False))
            
            # Extract and set metadata from pre-transform data
            for _, row in warnings_frame.iterrows():
                try:
                    warning_subframe = warnings_frame[
                        (warnings_frame["table_name"] == row["table_name"])
                    ]
                    self.tables[row["table_name"]].set_warning_messages(warning_subframe)
                except Exception as e:
                    print(f"SL300 Warning: Could not set metadata for table '{row['table_name']}': {e}")
            
        except Exception as e:
            print(f"SL010 Could not extract warning messages: {e}")
        
        print("Transformslib will now attempt to read in the data types...")
        
        print("Transformslib will now attempt to read in the list of person keys...")
        
        print("Transformslib will now attempt to read in the list of known entity ids...")
        #ids = gather_supply_ids(spark=spark)
        #if len(ids) > 0:
        if len(ent_keys) > 0:
            print("")
            print("Loading the entity map...")
            vals = list(ent_keys.values())
            #print(vals)
            ent_map = load_ent_map(vals)
            self.tables.append(ent_map)
            self.named_tables[ent_map.table_name] = ent_map 
        else:
            print("No entity map IDs found in the supply, skipping entity map load.")
        
        print("Loaded the following tables: ")
        print(self.named_tables)
        

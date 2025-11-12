import os
import shutil
import json
from typing import Dict, Any
from adaptiveio import load_json
from transformslib.tables.metaframe import MetaFrame
from transformslib.transforms.reader import transform_log_loc, does_transform_log_exist
from transformslib.tables.sv import SchemaValidator, SchemaValidationError
from .collection import TableCollection

def get_run_state() -> str:
    """
    Return the path location of the input payload.

    Returns:
        str: The payload path.
    """
    path = os.environ.get("TNSFRMS_JOB_STATE", "../test_tables")
    job_id = os.environ.get("TNSFRMS_JOB_ID", 1)
    run_id = os.environ.get("TNSFRMS_RUN_ID", 1)
    #format the path for job_id and run_id
    path = path.replace("{job_id}", str(job_id)).replace("{run_id}", str(run_id))
    path = path.replace("{prodtest}", os.environ.get("TNSFRMS_PROD", "prod"))

    return path

def get_supply_file(table_name: str) -> str:
    """
    Return the path location of the input payload.

    Args: table_name (str): The name of the table to load.

    Returns:
        str: The payload path.
    """
    base_path = os.environ.get("TNSFRMS_JOB_PATH", "../test_tables")
    job_id = os.environ.get("TNSFRMS_JOB_ID", 1)
    run_id = os.environ.get("TNSFRMS_RUN_ID", 1)
    #format the path for job_id
    path = base_path.replace("{job_id}", str(job_id))
    path = path.replace("{prodtest}", os.environ.get("TNSFRMS_PROD", "prod"))
    path = path.replace("{run_id}", str(run_id))
    path = path.replace("{tablename}", table_name)
    
    print(path)
    
    return path

def get_table_names_from_run_state(run_state: Dict[str, Any]) -> list:
    """
    Extract a unique, sorted list of table_name values from a run_state dict
    (looks under all_files -> data_files, map_files, enum_file, schema_file, other_files).
    """
    names = set()
    all_files = run_state.get("all_files", {})
    for section in ("data_files"):
        for entry in all_files.get(section, []):
            tn = entry.get("table_name")
            if tn:
                names.add(tn)
    return sorted(names)

def load_single_table(data: Dict[str, Any],
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
    file_path = data["input_file_path"]

    print(f"Loading table '{name}' from {file_path} (format: {data['file_format']})")

    table = MetaFrame.load(
            path=file_path,
            format=data["file_format"],
            frame_type="pyspark",
            spark=spark
    )

    # Perform schema validation if enabled and dtypes are provided
    if enable_schema_validation and "dtypes" in data:
        try:
            print(f"Validating schema for table '{name}'...")
            dtypes = data["dtypes"]
            # Print schema summary for transparency
            schema_summary = SchemaValidator.get_schema_summary(dtypes)
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
            print(f"Schema validation failed for table '{name}': {e}")
            raise e
        except Exception as e:
            print(f"Warning: Unexpected error during schema validation for table '{name}': {e}")
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
                "input_file_path": "path/to/data.parquet",
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
    
    def __init__(self, sample_frac: float = None, sample_rows: int = None, seed: int = None, spark=None, enable_schema_validation: bool = True):
        """
        Initialise a SupplyLoad instance with a JSON configuration file.

        This constructor loads the JSON configuration file and automatically creates
        MetaFrame instances for each supply item defined in the configuration.
        All tables are loaded as PySpark DataFrames by default.

        Args:
            sample_frac (float, optional): Fraction of rows to sample (0 < frac <= 1).
            sample_rows (int, optional): Number of rows to sample.
            seed (int, optional): Random seed for reproducibility.
            spark: SparkSession object required for loading PySpark DataFrames. Defaults to None.
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
            raise ValueError("Transform has been run beforehand, please CLEAR previous result or use new run id")

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

        self.load_supplies(spark=spark)

    def load_supplies(self, spark=None):
        """
        Load supply data from the JSON configuration file.

        This method reads either a sampling_state.json (new sampling input method)
        configuration file and creates MetaFrame instances for each supply item. It validates that each 
        supply item has the required fields, loads the data using the specified format and path, and 
        optionally applies sampling. For the new sampling system, schema validation is performed if enabled.

        Args:
            spark: SparkSession object required for PySpark operations. Defaults to None.

        Raises:
            FileNotFoundError: If the JSON configuration file doesn't exist.
            ValueError: If the JSON format is invalid or missing required fields.
            SchemaValidationError: If schema validation fails (only for new sampling system).
            Exception: If there are issues loading any of the data files.

        Example:

            >>> supply_loader = SupplyLoad(job_id=1, spark=spark)  # New sampling input method
        """
        print(f"Starting supply loading from: {self.supply_load_src}")
        
        try:
            print("reading table names from state file")
            run_state = load_json(self.supply_load_src, spark=spark)
            table_names = get_table_names_from_run_state(run_state)
            #print(table_names)

            print("for each table, loading the supply file")
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
                        mt = load_single_table(
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
                        raise ValueError("Unrecognized JSON format: expected 'table_name' key")
                except Exception as e:
                    print(f"Error SL001 loading table '{t}': {e}")
                    raise e

        except FileNotFoundError:
            raise FileNotFoundError(f"Supply JSON file not found at {self.supply_load_src}")
        
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in supply load file")
        
        print(f"Successfully loaded {len(self.tables)} tables")
        
        print("Loaded the following tables: ")
        print(self.named_tables)

    @staticmethod
    def wipe_run_outputs(job_id: int, run_id: int = None):
        """
        Wipe the outputs and results of a previous run for a given job_id (and optionally run_id).
        This removes the transform log and any output files/directories associated with the run.
        """
        # Remove output files in test_tables/output/job_<job_id> or similar
        output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../test_tables/output'))
        if os.path.exists(output_dir):
            for entry in os.listdir(output_dir):
                if entry.startswith('job_') and entry == f'job_{job_id}':
                    full_path = os.path.join(output_dir, entry)
                    if os.path.isdir(full_path):
                        shutil.rmtree(full_path)
                        print(f"Removed output directory: {full_path}")
                    else:
                        os.remove(full_path)
                        print(f"Removed output file: {full_path}")
        else:
            print(f"No output directory found at: {output_dir}")

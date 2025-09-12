import json
from typing import Dict, Any
from transformslib.tables.metaframe import MetaFrame
from transformslib.tables.collections.collection import TableCollection
from transformslib.transforms.reader import transform_log_loc, does_transform_log_exist
from transformslib.tables.schema_validator import SchemaValidator, SchemaValidationError

def get_supply_file(job_id:int, run_id:int = None) -> str:
    """
    Return the path location of the input payload.

    Args:
        job_id (int): A job id to get the path of configuration file containing supply definitions.
        run_id (int, optional): A run id to get the path of configuration file containing supply definitions. If None, returns path to sampling_state.json for the new sampling input method.

    Returns:
        str: The payload path.
    """
    
    if run_id is None:
        # New sampling input method - use sampling_state.json
        print(f"Using new sampling input method for job_id={job_id} (no run_id specified)")
        return f"../test_tables/job_{job_id}/sampling_state.json"
    else:
        return legacy_get_payload_file(job_id, run_id)


def legacy_get_payload_file(job_id: int, run_id: int) -> str:
    """
    Return the path location of the legacy input payload (payload.json).

    Args:
        job_id (int): A job id to get the path of configuration file containing supply definitions.
        run_id (int): A run id to get the path of configuration file containing supply definitions.

    Returns:
        str: The legacy payload path.
    """
    print(f"[LEGACY] Using legacy payload method for job_id={job_id}, run_id={run_id}")
    return f"../test_tables/job_{job_id}/payload.json"


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

        # Apply sampling if requested
        if sample:
            table.sample(n=sample_rows, frac=sample_frac, seed=seed)

        tables.append(table)
        named_tables[name] = table


def load_from_sampling_state(data: Dict[str, Any], tables: list, named_tables: Dict[str, Any],
                            sample: bool, sample_rows: int = None, sample_frac: float = None,
                            seed: int = None, spark=None, enable_schema_validation: bool = True) -> None:
    """
    Load supplies from new sampling_state.json format with optional schema validation.
    
    Args:
        data (Dict[str, Any]): The parsed JSON data from sampling_state.json
        tables (list): List to append loaded tables to
        named_tables (Dict[str, Any]): Dictionary to store named table references
        sample (bool): Whether to apply sampling to loaded tables
        sample_rows (int, optional): Number of rows to sample
        sample_frac (float, optional): Fraction of rows to sample
        seed (int, optional): Random seed for reproducible sampling
        spark: SparkSession object for PySpark operations
        enable_schema_validation (bool): Whether to perform schema validation. Defaults to True.

    Returns:
        None
    """
    print("Loading supplies from new sampling_state.json format")
    sample_files = data.get("sample_files", [])
    
    for item in sample_files:
        name = item.get("table_name")
        if not name:
            raise ValueError("Each sample file item must have a 'table_name' field")

        # Handle path normalization - sampling_state.json may use relative paths
        file_path = item["input_file_path"]

        print(f"Loading table '{name}' from {file_path} (format: {item['file_format']})")

        table = MetaFrame.load(
            path=file_path,
            format=item["file_format"],
            frame_type="pyspark",
            spark=spark
        )

        # Perform schema validation if enabled and dtypes are provided
        if enable_schema_validation and "dtypes" in item:
            try:
                print(f"Validating schema for table '{name}'...")
                dtypes = item["dtypes"]
                
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
                print(f"✓ Schema validation passed for table '{name}'")
                
            except SchemaValidationError as e:
                print(f"✗ Schema validation failed for table '{name}': {e}")
                # You can choose to raise the error or just warn
                # For now, we'll raise it to ensure data integrity
                raise e
            except Exception as e:
                print(f"Warning: Unexpected error during schema validation for table '{name}': {e}")
                # Continue loading even if schema validation has unexpected errors
        elif enable_schema_validation:
            print(f"Warning: No schema information (dtypes) found for table '{name}' - skipping validation")

        # Apply sampling if requested
        if sample:
            table.sample(n=sample_rows, frac=sample_frac, seed=seed)

        tables.append(table)
        named_tables[name] = table

class SupplyLoad(TableCollection):
    """
    A specialised collection manager for loading and managing supply data from JSON configuration files.

    This class extends TableCollection to provide automated loading of multiple data sources from a JSON configuration file. It supports two input methods:

    1. Legacy payload.json format (requires both job_id and run_id)
    2. New sampling input method using sampling_state.json (requires only job_id)
    
    The new sampling system includes schema validation capabilities that automatically verify
    loaded data against the expected schema defined in the dtypes field.
    
    Legacy JSON configuration should follow this structure:
    {
        "supply": [
            {
                "name": "table_name",
                "path": "path/to/data.parquet",
                "format": "parquet"
            },
        ]
    }
    
    New sampling state JSON configuration follows this structure:
    {
        "sample_files": [
            {
                "table_name": "table_name",
                "input_file_path": "path/to/data.csv",
                "file_format": "csv",
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
        >>> 
        >>> # Legacy method (schema validation not available)
        >>> supply_loader = SupplyLoad(job_id=1, run_id=2, spark=spark)

        >>> customers_table = supply_loader["customers"]
        >>> orders_table = supply_loader["orders"]

        >>> # Save events for all loaded tables
        >>> supply_loader.save_events()
    """
    
    def __init__(self, job_id:int, run_id:int = None, sample_frac: float = None, sample_rows: int = None, seed: int = None, spark=None, enable_schema_validation: bool = True):
        """
        Initialise a SupplyLoad instance with a JSON configuration file.

        This constructor loads the JSON configuration file and automatically creates
        MetaFrame instances for each supply item defined in the configuration.
        All tables are loaded as PySpark DataFrames by default.

        Args:
            job_id (int): A job id to get the path of configuration file containing supply definitions.
            run_id (int, optional): A run id to get the path of configuration file containing supply definitions. If None, uses the new sampling input method with sampling_state.json. If provided, uses the legacy payload.json format.
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
                       (Only applies when run_id is provided for legacy mode)
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
            >>> 
            >>> # Legacy method (schema validation not applied)
            >>> supply_loader = SupplyLoad(job_id=1, run_id=2, spark=spark)

            >>> print(f"Loaded {len(supply_loader)} tables")
        """
        
        # Initialise the parent class with empty tables list
        super().__init__(tables=[])

        #run parameters
        self.job = job_id
        self.run = run_id
        self.enable_schema_validation = enable_schema_validation
        
        #identify the load dir and payload loc
        self.supply_load_src = get_supply_file(job_id, run_id)
        
        #gather the source payload location
        # Only check transform log if run_id is provided (legacy mode)
        if run_id is not None:
            self.output_loc = transform_log_loc(job_id, run_id)
            if (does_transform_log_exist(job_id, run_id)):
                raise ValueError("Transform has been run beforehand, please CLEAR previous result or use new run id")
        else:
            # For new sampling input method, we don't track transform logs the same way
            self.output_loc = None

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

        This method reads either a payload.json (legacy) or sampling_state.json (new sampling input method)
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
            >>> supply_loader = SupplyLoad(job_id=1, run_id=2, spark=spark)  # Legacy method
        """
        print(f"Starting supply loading from: {self.supply_load_src}")
        
        try:
            with open(self.supply_load_src, 'r') as file:
                data = json.load(file)
                
                # Determine format based on the structure of the JSON file
                if "sample_files" in data:
                    # New sampling input method (sampling_state.json format)
                    # Schema validation is only available for the new system
                    load_from_sampling_state(
                        data=data, 
                        tables=self.tables, 
                        named_tables=self.named_tables,
                        sample=self.sample,
                        sample_rows=self.sample_rows,
                        sample_frac=self.sample_frac,
                        seed=self.seed,
                        spark=spark,
                        enable_schema_validation=self.enable_schema_validation
                    )
                elif "supply" in data:
                    # Legacy format (payload.json format)
                    # Schema validation is not applied to legacy system
                    if self.enable_schema_validation:
                        print("Note: Schema validation is not available for legacy payload.json format")
                    load_from_payload(
                        data=data,
                        tables=self.tables,
                        named_tables=self.named_tables,
                        sample=self.sample,
                        sample_rows=self.sample_rows,
                        sample_frac=self.sample_frac,
                        seed=self.seed,
                        spark=spark
                    )
                else:
                    raise ValueError("Unrecognized JSON format: expected either 'sample_files' or 'supply' key")

        except FileNotFoundError:
            raise FileNotFoundError(f"Supply JSON file not found at {self.supply_load_src}")
        
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in supply load file")
        
        print(f"Successfully loaded {len(self.tables)} tables")

    @staticmethod
    def wipe_run_outputs(job_id: int, run_id: int = None):
        """
        Wipe the outputs and results of a previous run for a given job_id (and optionally run_id).
        This removes the transform log and any output files/directories associated with the run.
        """
        import os
        import shutil
        from transformslib.transforms.reader import transform_log_loc

        # Remove transform log (legacy mode)
        if run_id is not None:
            log_path = transform_log_loc(job_id, run_id)
            if os.path.exists(log_path):
                os.remove(log_path)
                print(f"Removed transform log: {log_path}")
            else:
                print(f"No transform log found at: {log_path}")

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

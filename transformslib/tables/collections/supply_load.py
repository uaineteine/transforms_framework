import json
from transformslib.tables.metaframe import MetaFrame
from transformslib.tables.collections.collection import TableCollection
from transformslib.transforms.reader import transform_log_loc, does_transform_log_exist

def get_payload_file(job_id:int, run_id:int = None) -> str:
    """
    Return the path location of the input payload.
    
    Args:
        job_id (int): A job id to get the path of configuration file containing supply definitions.
        run_id (int, optional): A run id to get the path of configuration file containing supply definitions.
                               If None, returns path to sampling_state.json for the new sampling input method.
        
    Returns:
        string of the payload path
    """
    
    if run_id is None:
        # New sampling input method - use sampling_state.json
        return f"test_tables/job_{job_id}/sampling_state.json"
    else:
        # Legacy method - use payload.json
        return f"test_tables/job_{job_id}/payload.json"

class SupplyLoad(TableCollection):
    """
    A specialised collection manager for loading and managing supply data from JSON configuration files.
    
    This class extends TableCollection to provide automated loading of multiple data sources
    from a JSON configuration file. It supports two input methods:
    
    1. Legacy payload.json format (requires both job_id and run_id)
    2. New sampling input method using sampling_state.json (requires only job_id)
    
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
            }
        ]
    }
    
    Attributes:
        supply_load_src (str): The path to the JSON configuration file.
        job (int): The job ID for the current operation.
        run (int): The run ID for the current operation (None for new sampling input method).
    
    Example:
        >>> # New sampling input method (job_id only)
        >>> supply_loader = SupplyLoad(job_id=1, spark=spark)
        >>> 
        >>> # Legacy method (job_id and run_id)
        >>> supply_loader = SupplyLoad(job_id=1, run_id=2, spark=spark)
        >>> 
        >>> customers_table = supply_loader["customers"]
        >>> orders_table = supply_loader["orders"]
        >>> 
        >>> # Save events for all loaded tables
        >>> supply_loader.save_events()
    """
    
    def __init__(self, job_id:int, run_id:int = None, sample_frac: float = None, sample_rows: int = None, seed: int = None, spark=None):
        """
        Initialise a SupplyLoad instance with a JSON configuration file.
        
        This constructor loads the JSON configuration file and automatically creates
        MetaFrame instances for each supply item defined in the configuration.
        All tables are loaded as PySpark DataFrames by default.

        Args:
            job_id (int): A job id to get the path of configuration file containing supply definitions.
            run_id (int, optional): A run id to get the path of configuration file containing supply definitions.
                                   If None, uses the new sampling input method with sampling_state.json.
                                   If provided, uses the legacy payload.json format.
            sample_frac (float, optional): Fraction of rows to sample (0 < frac <= 1).
            sample_rows (int, optional): Number of rows to sample.
            seed (int, optional): Random seed for reproducibility.
            spark: SparkSession object required for loading PySpark DataFrames. Defaults to None.

        Raises:
            FileNotFoundError: If the JSON configuration file doesn't exist.
            ValueError: If the output transform file already exists suggesting the run has been made before.
                       (Only applies when run_id is provided for legacy mode)
            Exception: If there are issues loading any of the data files.

        Example:
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.appName("SupplyLoad").getOrCreate()
            >>> 
            >>> # New sampling input method
            >>> supply_loader = SupplyLoad(job_id=1, spark=spark)
            >>> 
            >>> # Legacy method
            >>> supply_loader = SupplyLoad(job_id=1, run_id=2, spark=spark)
            >>> 
            >>> print(f"Loaded {len(supply_loader)} tables")
        """
        
        # Initialise the parent class with empty tables list
        super().__init__(tables=[])

        #run parameters
        self.job = job_id
        self.run = run_id
        
        #identify the load dir and payload loc
        self.supply_load_src = get_payload_file(job_id, run_id)
        
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

        self.load_supplies(spark=spark)

    def load_supplies(self, spark=None):
        """
        Load supply data from the JSON configuration file.
        
        This method reads either a payload.json (legacy) or sampling_state.json (new sampling input method)
        configuration file and creates MetaFrame instances for each supply item. It validates that each 
        supply item has the required fields, loads the data using the specified format and path, and 
        optionally applies sampling.

        Args:
            spark: SparkSession object required for PySpark operations. Defaults to None.

        Raises:
            FileNotFoundError: If the JSON configuration file doesn't exist.
            ValueError: If the JSON format is invalid or missing required fields.
            Exception: If there are issues loading any of the data files.

        Example:
            >>> supply_loader = SupplyLoad(job_id=1, spark=spark)  # New sampling input method
            >>> supply_loader = SupplyLoad(job_id=1, run_id=2, spark=spark)  # Legacy method
        """
        try:
            with open(self.supply_load_src, 'r') as file:
                data = json.load(file)
                
                # Determine format based on the structure of the JSON file
                if "sample_files" in data:
                    # New sampling input method (sampling_state.json format)
                    self._load_from_sampling_state(data, spark)
                elif "supply" in data:
                    # Legacy format (payload.json format)
                    self._load_from_payload(data, spark)
                else:
                    raise ValueError("Unrecognized JSON format: expected either 'sample_files' or 'supply' key")

        except FileNotFoundError:
            raise FileNotFoundError(f"Supply JSON file not found at {self.supply_load_src}")
        
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in supply load file")

    def _load_from_payload(self, data, spark):
        """Load supplies from legacy payload.json format."""
        supply = data.get("supply", [])
        for item in supply:
            name = item.get("name")
            if not name:
                raise ValueError("Each supply item must have a 'name' field")

            table = MetaFrame.load(
                path=item["path"],
                format=item["format"],
                frame_type="pyspark",
                spark=spark
            )

            # Apply sampling if requested
            if self.sample:
                table.sample(n=self.sample_rows, frac=self.sample_frac, seed=self.seed)

            self.tables.append(table)
            self.named_tables[name] = table

    def _load_from_sampling_state(self, data, spark):
        """Load supplies from new sampling_state.json format."""
        sample_files = data.get("sample_files", [])
        for item in sample_files:
            name = item.get("table_name")
            if not name:
                raise ValueError("Each sample file item must have a 'table_name' field")

            # Handle path normalization - sampling_state.json may use relative paths
            file_path = item["input_file_path"]
            if file_path.startswith("../"):
                # Remove the ../ prefix to make it relative to current directory
                file_path = file_path[3:]

            table = MetaFrame.load(
                path=file_path,
                format=item["file_format"],
                frame_type="pyspark",
                spark=spark
            )

            # Apply sampling if requested
            if self.sample:
                table.sample(n=self.sample_rows, frac=self.sample_frac, seed=self.seed)

            self.tables.append(table)
            self.named_tables[name] = table

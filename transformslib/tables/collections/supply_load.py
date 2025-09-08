import json
from transformslib.tables.metaframe import MetaFrame
from transformslib.tables.collections.collection import TableCollection

class SupplyLoad(TableCollection):
    """
    A specialised collection manager for loading and managing supply data from JSON configuration files.
    
    This class extends TableCollection to provide automated loading of multiple data sources
    from a JSON configuration file. It's designed for scenarios where you need to load
    multiple related datasets (supplies) from a single configuration source.
    
    The JSON configuration should follow this structure:
    {
        "supply": [
            {
                "name": "table_name",
                "path": "path/to/data.parquet",
                "format": "parquet"
            },
            ...
        ]
    }
    
    Attributes:
        supply_load_src (str): The path to the JSON configuration file.
        
    Example:
        >>> # JSON file: supply_config.json
        >>> # {
        >>> #     "supply": [
        >>> #         {"name": "customers", "path": "data/customers.parquet", "format": "parquet"},
        >>> #         {"name": "orders", "path": "data/orders.parquet", "format": "parquet"}
        >>> #     ]
        >>> # }
        >>> 
        >>> supply_loader = SupplyLoad("supply_config.json", spark)
        >>> customers_table = supply_loader["customers"]
        >>> orders_table = supply_loader["orders"]
        >>> 
        >>> # Save events for all loaded tables
        >>> supply_loader.save_events()
    """
    
    def __init__(self, json_loc: str, sample_frac: float = None, sample_rows: int = None, seed: int = None, spark=None):
        """
        Initialise a SupplyLoad instance with a JSON configuration file.
        
        This constructor loads the JSON configuration file and automatically creates
        MetaFrame instances for each supply item defined in the configuration.
        All tables are loaded as PySpark DataFrames by default.

        Args:
            json_loc (str): Path to the JSON configuration file containing supply definitions.
            sample_frac (float, optional): Fraction of rows to sample (0 < frac <= 1).
            sample_rows (int, optional): Number of rows to sample.
            seed (int, optional): Random seed for reproducibility.
            spark: SparkSession object required for loading PySpark DataFrames. Defaults to None.

        Raises:
            ValueError: If the JSON path is empty.
            FileNotFoundError: If the JSON configuration file doesn't exist.
            ValueError: If the JSON format is invalid or missing required fields.
            Exception: If there are issues loading any of the data files.

        Example:
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.appName("SupplyLoad").getOrCreate()
            >>> 
            >>> supply_loader = SupplyLoad("config/supply_data.json", spark)
            >>> print(f"Loaded {len(supply_loader)} tables")
        """
        if not json_loc:
            raise ValueError("Supply JSON path cannot be empty")
        
        # Initialise the parent class with empty tables list
        super().__init__(tables=[])

        self.job = 1
        self.run = 1
        self.payload_dir="../test_tables"
        self.supply_load_src = json_loc

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
        
        This method reads the JSON configuration file and creates MetaFrame instances
        for each supply item. It validates that each supply item has the required fields,
        loads the data using the specified format and path, and optionally applies sampling.

        Args:
            spark: SparkSession object required for PySpark operations. Defaults to None.

        Raises:
            FileNotFoundError: If the JSON configuration file doesn't exist.
            ValueError: If the JSON format is invalid or missing required fields.
            Exception: If there are issues loading any of the data files.

        Example:
            >>> supply_loader = SupplyLoad("config.json", spark)
            >>> supply_loader.load_supplies(sample_frac=0.1, seed=42, spark=spark)
        """
        try:
            with open(self.supply_load_src, 'r') as file:
                data = json.load(file)
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

        except FileNotFoundError:
            raise FileNotFoundError(f"Supply JSON file not found at {self.supply_load_src}")
        
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in supply load file")


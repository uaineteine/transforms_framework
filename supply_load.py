import json
from pipeline_table import PipelineTable

class SupplyLoad:
    """Class to handle supply load information from a JSON file."""
    def __init__(self, json_loc: str, spark=None):
        if not json_loc:
            raise ValueError("Supply JSON path cannot be empty")
        
        self.supply_load_src = json_loc
        self.tables = []              # List of all tables
        self.named_tables = {}        # Dict to access tables by name
        self.load_supplies(spark)

    def load_supplies(self, spark=None):
        """Load supply data from the JSON file."""
        try:
            with open(self.supply_load_src, 'r') as file:
                data = json.load(file)
                supply = data.get("supply", [])
                for item in supply:
                    name = item.get("name")
                    if not name:
                        raise ValueError("Each supply item must have a 'name' field")

                    table = PipelineTable.load(
                        path=item["path"],
                        format=item["format"],
                        frame_type="pyspark",
                        spark=spark
                    )
                    self.tables.append(table)
                    self.named_tables[name] = table

        except FileNotFoundError:
            raise FileNotFoundError(f"Supply JSON file not found at {self.supply_load_src}")
        
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in supply load file")

    def get_table(self, name: str):
        """Retrieve a table by its name."""
        if name not in self.named_tables:
            raise KeyError(f"Table '{name}' not found in supply load")
        return self.named_tables[name]

    def __getitem__(self, name: str):
        """Allow dictionary-style access to tables by name."""
        return self.get_table(name)

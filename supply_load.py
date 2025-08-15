import json
from pipeline_table import PipelineTable, PipelineTables

class SupplyLoad(PipelineTables):
    """Class to handle supply load information from a JSON file."""
    
    def __init__(self, json_loc: str, spark=None):
        if not json_loc:
            raise ValueError("Supply JSON path cannot be empty")
        
        # Initialize the parent class with empty tables list
        super().__init__(tables=[])
        
        self.supply_load_src = json_loc
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

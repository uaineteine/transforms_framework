import json
from pipeline_table import PipelineTable

class SupplyLoad:
    """Class to handle supply load information from a JSON file."""
    def __init__(self, json_loc:str):
        if json_loc = "":
            raise ValueError("Supply JSON path cannot be empty")
        
        self.supply_load_src = json_loc

        #return whole list of PipelineTables for working with the supply load
        self.tables = [] # List to hold supply load tables

    def load_supplies(self):
        """Load supply data from the JSON file."""
        try:
            with open(self.supply_load_src, 'r') as file:
                data = json.load(file)
                self.supply = data.get("supply", [])
                for item in self.supply:
                    table = PipelineTable(item["name"], item["format"], item["path"])
                    self.tables.append(table)
        except FileNotFoundError:
            raise FileNotFoundError(f"Supply JSON file not found at {self.supply_load_src}")
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in supply load file"

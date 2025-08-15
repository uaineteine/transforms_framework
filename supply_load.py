import json
from pipeline_table import PipelineTable

class PipelineTables:
    """Base class to handle a collection of PipelineTable objects."""
    
    def __init__(self, tables: list[PipelineTable] = None):
        self.tables = tables if tables is not None else []
        self.named_tables = {}        # Dict to access tables by name
        
        # Initialize named_tables if tables are provided
        if tables:
            for table in tables:
                if hasattr(table, 'name') and table.name:
                    self.named_tables[table.name] = table

    def get_table(self, name: str):
        """Retrieve a table by its name."""
        if name not in self.named_tables:
            raise KeyError(f"Table '{name}' not found")
        return self.named_tables[name]

    def __getitem__(self, name: str):
        """Allow dictionary-style access to tables by name."""
        return self.get_table(name)

    def __setitem__(self, name: str, table):
        """Allow dictionary-style assignment of tables by name."""
        if not name:
            raise ValueError("Table name cannot be empty")
        
        # If the table already exists, update it
        if name in self.named_tables:
            # Remove the old table from the tables list
            old_table = self.named_tables[name]
            if old_table in self.tables:
                self.tables.remove(old_table)
        
        # Add the new table
        self.named_tables[name] = table
        self.tables.append(table)

    def __delitem__(self, name: str):
        """Allow dictionary-style deletion of table collections by name."""
        if name not in self.named_tables:
            raise KeyError(f"Table '{name}' not found")
        
        table = self.named_tables[name]
        if table in self.tables:
            self.tables.remove(table)
        del self.named_tables[name]

    def __contains__(self, name: str):
        """Check if a table with the given name exists."""
        return name in self.named_tables

    def save_events(self, table_names: list[str] = None):
        """
        Save events for all tables or specific tables.
        
        :param table_names: List of table names to save events for. If None, saves events for all tables.
        :return: None
        """
        if table_names is None:
            # Save events for all tables
            for table in self.tables:
                table.save_events()
        else:
            # Save events for specific tables
            for name in table_names:
                if name not in self.named_tables:
                    raise KeyError(f"Table '{name}' not found")
                self.named_tables[name].save_events()


class SupplyLoad(PipelineTables):
    """Class to handle supply load information from a JSON file."""
    
    def __init__(self, json_loc: str, spark=None):
        if not json_loc:
            raise ValueError("Supply JSON path cannot be empty")
        
        # Initialize the parent class
        super().__init__()
        
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
                    # Create a PipelineTables instance for this single table
                    table_collection = PipelineTables()
                    table_collection.tables = [table]
                    table_collection.named_tables[name] = table
                    
                    self.tables.append(table_collection)
                    self.named_tables[name] = table_collection

        except FileNotFoundError:
            raise FileNotFoundError(f"Supply JSON file not found at {self.supply_load_src}")
        
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in supply load file")

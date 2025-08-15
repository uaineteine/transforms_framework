import os
from pipeline_event import PipelineEvent

#module imports
from metaframe import MetaFrame
from uainepydat.frameverifier import FrameTypeVerifier

class PipelineTable(MetaFrame): 
    """
    Class that holds a metaframe and a list of events to log.
    """

    def __init__(self, metaframe:MetaFrame):
        """
        Initialize a class type with a new event log attached.

        :param metaframe: MetaFrame (PySpark, Pandas, or Polars).
        """
        #call metaframe constructor
        super().__init__(metaframe.df, metaframe.src_path, metaframe.table_name, metaframe.frame_type)

        #intialise events as a list
        self.events: List[PipelineEvent] = []

        #store a version number
        self.pipeline_table_version = "0.1.0"
    
    def add_event(self, event:PipelineEvent):
        """
        Append the events list
        """
        self.events.append(event)

    @staticmethod
    def load(path:str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, spark=None):
        """
        Load a DataFrame from the given path and return a Metaframe.

        :param path: Path to the data file.
        :param format: File format (default: 'parquet').
        :param table_name: Optional table name.
        :param frame_type: Type of DataFrame ('pyspark', 'pandas', 'polars').
        :param spark: SparkSession object (required for PySpark).
        :return: MetaFrame instance.
        """
        #print(table_name)
        mf = MetaFrame.load(path, format, table_name, frame_type, spark)

        ptable = PipelineTable(mf)
        event = PipelineEvent(event_type="load", message=f"Loaded table from {path} as {format} ({frame_type})", description=f"Loaded {table_name} from {path} with version {ptable.pipeline_table_version}")
        ptable.add_event(event)
        return ptable

    def save_events(self) -> None:
        """
        Save the events to a JSON file in the events_log directory.

        :return: None
        """
        os.makedirs("events_log", exist_ok=True)
        
        log_path = f"events_log/job_1/{self.table_name}_events.json"
        for event in self.events:
            event.log_location = log_path
            event.log()
        
        print(f"Events saved to {log_path}")

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
                
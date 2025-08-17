import os
import fnmatch
from typing import List, Union
from pipeline_event import PipelineEvent

#module imports
from metaframe import MetaFrame
from uainepydat.frameverifier import FrameTypeVerifier

class PipelineTable(MetaFrame): 
    """
    A specialised class that extends MetaFrame to include event logging capabilities for data pipeline operations.
    
    This class combines the functionality of a MetaFrame (which handles different DataFrame types like PySpark, 
    Pandas, or Polars) with an event logging system that tracks all operations performed on the data.
    
    Attributes:
        events (List[PipelineEvent]): A list of events that have been logged during the pipeline operations.
        pipeline_table_version (str): Version identifier for the pipeline table implementation.
        
    Example:
        >>> # Create a PipelineTable from an existing MetaFrame
        >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pyspark", spark)
        >>> pt = PipelineTable(mf)
        >>> 
        >>> # Add custom events
        >>> event = PipelineEvent("transform", "Applied filter", "Filtered rows where column > 10")
        >>> pt.add_event(event)
        >>> 
        >>> # Save all events to log files
        >>> pt.save_events()
    """

    def __init__(self, metaframe: MetaFrame, inherit_events: List[PipelineEvent] = None):
        """
        Initialize a PipelineTable with a MetaFrame and optional event log.

        Args:
            metaframe (MetaFrame): A MetaFrame object containing the DataFrame and metadata.
                                 Must be a valid MetaFrame instance with df, src_path, 
                                 table_name, and frame_type attributes.
            inherit_events (List[PipelineEvent], optional): List of events to inherit from
                                 another PipelineTable. Defaults to None.

        Raises:
            TypeError: If metaframe is not a MetaFrame instance.
            AttributeError: If metaframe is missing required attributes.

        Example:
            >>> mf = MetaFrame(df, "path/to/data.parquet", "my_table", "pyspark")
            >>> pt = PipelineTable(mf)
            >>> # With inherited events
            >>> pt_with_events = PipelineTable(mf, inherit_events=existing_events)
        """
        #call metaframe constructor
        super().__init__(metaframe.df, metaframe.src_path, metaframe.table_name, metaframe.frame_type)

        #initialize events as a list, optionally inheriting from existing events
        self.events: List[PipelineEvent] = inherit_events.copy() if inherit_events else []

        #store a version number
        self.pipeline_table_version = "0.1.0"
    
    def add_event(self, event: PipelineEvent):
        """
        Append an event to the internal events list for logging purposes.
        
        This method adds a PipelineEvent to the tracking system, which will be saved
        when save_events() is called.

        Args:
            event (PipelineEvent): The event object to add to the log. Must be a valid
                                 PipelineEvent instance with event_type, message, and
                                 description attributes.

        Raises:
            TypeError: If event is not a PipelineEvent instance.

        Example:
            >>> event = PipelineEvent("transform", "Data cleaning", "Removed null values")
            >>> pt.add_event(event)
        """
        self.events.append(event)

    @staticmethod
    def load(path: str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, spark=None):
        """
        Load a DataFrame from the given path and return a PipelineTable with an initial load event.

        This static method creates a PipelineTable from a data file, automatically logging
        the load operation as the first event in the pipeline.

        Args:
            path (str): Path to the data file to load.
            format (str, optional): File format of the data. Defaults to "parquet".
                                  Supported formats: "parquet", "csv", "json", etc.
            table_name (str, optional): Name to assign to the table. Defaults to "".
            frame_type (str, optional): Type of DataFrame to create. Defaults to "pyspark".
                                      Supported types: "pyspark", "pandas", "polars".
            spark: SparkSession object (required for PySpark frame_type). Defaults to None.

        Returns:
            PipelineTable: A new PipelineTable instance with the loaded data and an initial load event.

        Raises:
            FileNotFoundError: If the specified path does not exist.
            ValueError: If the format or frame_type is not supported.
            Exception: If there are issues loading the data or creating the MetaFrame.

        Example:
            >>> # Load a PySpark DataFrame
            >>> pt = PipelineTable.load("data.parquet", "parquet", "my_table", "pyspark", spark)
            >>> 
            >>> # Load a Pandas DataFrame
            >>> pt = PipelineTable.load("data.csv", "csv", "my_table", "pandas")
        """
        #print(table_name)
        mf = MetaFrame.load(path, format, table_name, frame_type, spark)

        ptable = PipelineTable(mf)
        event = PipelineEvent(event_type="load", message=f"Loaded table from {path} as {format} ({frame_type})", description=f"Loaded {table_name} from {path} with version {ptable.pipeline_table_version}")
        ptable.add_event(event)
        return ptable

    def save_events(self) -> None:
        """
        Save all logged events to a JSON file in the events_log directory.

        This method creates the events_log directory structure and saves each event
        as a separate JSON file. The events are organised by job and table name.

        Returns:
            None

        Raises:
            OSError: If there are issues creating directories or writing files.
            Exception: If there are issues serializing events to JSON.

        Example:
            >>> pt = PipelineTable.load("data.parquet", "parquet", "my_table")
            >>> pt.add_event(PipelineEvent("transform", "Filtered data", "Applied age > 18 filter"))
            >>> pt.save_events()  # Saves to events_log/job_1/my_table_events.json
        """
        os.makedirs("events_log", exist_ok=True)
        
        log_path = f"events_log/job_1/{self.table_name}_events.json"
        for event in self.events:
            event.log_location = log_path
            event.log()
        
        print(f"Events saved to {log_path}")

class PipelineTables:
    """
    A collection manager for multiple PipelineTable objects with dictionary-like access.
    
    This class provides a convenient way to manage multiple PipelineTable instances,
    allowing access by name through dictionary-style operations. It maintains both
    a list of tables and a dictionary for named access, ensuring consistency between
    the two data structures.
    
    Attributes:
        tables (list[PipelineTable]): List of all PipelineTable instances in the collection.
        named_tables (dict): Dictionary mapping table names to PipelineTable instances.
        
    Example:
        >>> # Create an empty collection
        >>> pt_collection = PipelineTables()
        >>> 
        >>> # Add tables
        >>> pt1 = PipelineTable.load("data1.parquet", "parquet", "table1")
        >>> pt2 = PipelineTable.load("data2.parquet", "parquet", "table2")
        >>> pt_collection["table1"] = pt1
        >>> pt_collection["table2"] = pt2
        >>> 
        >>> # Access tables
        >>> table = pt_collection["table1"]
        >>> table_count = len(pt_collection)
        >>> 
        >>> # Select tables by prefix
        >>> clus_tables = pt_collection.select_by_names("clus_*")
        >>> 
        >>> # Select tables by range
        >>> specific_tables = pt_collection.select_by_names("table1", "table3")
        >>> 
        >>> # Save all events
        >>> pt_collection.save_events()
    """
    
    def __init__(self, tables: list[PipelineTable] = None):
        """
        Initialize a PipelineTables collection.

        Args:
            tables (list[PipelineTable], optional): Initial list of PipelineTable instances.
                                                   Defaults to None (empty collection).

        Example:
            >>> # Empty collection
            >>> pt_collection = PipelineTables()
            >>> 
            >>> # Collection with initial tables
            >>> tables = [pt1, pt2, pt3]
            >>> pt_collection = PipelineTables(tables)
        """
        self.tables = tables if tables is not None else []
        self.named_tables = {}        # Dict to access tables by name
        
        # Initialize named_tables if tables are provided
        if tables:
            for table in tables:
                if hasattr(table, 'table_name') and table.table_name:
                    self.named_tables[table.table_name] = table

    def select_by_names(self, *name_patterns: str) -> 'PipelineTables':
        """
        Select tables by name patterns, supporting wildcards and exact matches.
        
        This method creates a new PipelineTables instance containing only the tables
        that match the specified name patterns. It supports:
        - Exact name matches: "table1", "table2"
        - Wildcard patterns: "clus_*", "*_2023", "table*"
        - Multiple patterns: "clus_*", "table1", "table3"
        
        Args:
            *name_patterns (str): Variable number of name patterns to match against.
                                Can be exact names or wildcard patterns.
        
        Returns:
            PipelineTables: A new PipelineTables instance containing only the matching tables.
        
        Raises:
            ValueError: If no name patterns are provided.
        
        Example:
            >>> # Select tables by prefix
            >>> clus_tables = pt.select_by_names("clus_*")
            >>> 
            >>> # Select specific tables
            >>> specific_tables = pt.select_by_names("table1", "table3")
            >>> 
            >>> # Select tables by multiple patterns
            >>> mixed_tables = pt.select_by_names("clus_*", "table1", "*_2023")
        """
        if not name_patterns:
            raise ValueError("At least one name pattern must be provided")
        
        matching_tables = []
        
        for table in self.tables:
            table_name = getattr(table, 'table_name', '')
            if not table_name:
                continue
                
            # Check if table name matches any of the patterns
            for pattern in name_patterns:
                if fnmatch.fnmatch(table_name, pattern):
                    matching_tables.append(table)
                    break  # Once matched, no need to check other patterns
        
        return PipelineTables(matching_tables)

    def select_by_prefix(self, prefix: str) -> 'PipelineTables':
        """
        Select tables that start with the specified prefix.
        
        This is a convenience method that uses select_by_names internally.
        
        Args:
            prefix (str): The prefix to match against table names.
        
        Returns:
            PipelineTables: A new PipelineTables instance containing only tables with matching prefix.
        
        Example:
            >>> # Select all tables starting with "clus_"
            >>> clus_tables = pt.select_by_prefix("clus_")
        """
        return self.select_by_names(f"{prefix}*")

    def select_by_suffix(self, suffix: str) -> 'PipelineTables':
        """
        Select tables that end with the specified suffix.
        
        This is a convenience method that uses select_by_names internally.
        
        Args:
            suffix (str): The suffix to match against table names.
        
        Returns:
            PipelineTables: A new PipelineTables instance containing only tables with matching suffix.
        
        Example:
            >>> # Select all tables ending with "_2023"
            >>> tables_2023 = pt.select_by_suffix("_2023")
        """
        return self.select_by_names(f"*{suffix}")

    def select_by_range(self, start_name: str, end_name: str) -> 'PipelineTables':
        """
        Select tables with names that fall within a lexicographic range.
        
        Args:
            start_name (str): The starting name (inclusive).
            end_name (str): The ending name (inclusive).
        
        Returns:
            PipelineTables: A new PipelineTables instance containing only tables within the range.
        
        Example:
            >>> # Select tables with names between "table1" and "table5"
            >>> range_tables = pt.select_by_range("table1", "table5")
        """
        matching_tables = []
        
        for table in self.tables:
            table_name = getattr(table, 'table_name', '')
            if not table_name:
                continue
                
            if start_name <= table_name <= end_name:
                matching_tables.append(table)
        
        return PipelineTables(matching_tables)

    def get_table_names(self) -> List[str]:
        """
        Get a list of all table names in the collection.
        
        Returns:
            List[str]: List of all table names.
        
        Example:
            >>> names = pt.get_table_names()
            >>> print(names)  # ['table1', 'table2', 'clus_data', ...]
        """
        return list(self.named_tables.keys())

    def filter_tables(self, filter_func) -> 'PipelineTables':
        """
        Filter tables using a custom filter function.
        
        Args:
            filter_func (callable): A function that takes a PipelineTable and returns True/False.
        
        Returns:
            PipelineTables: A new PipelineTables instance containing only tables that pass the filter.
        
        Example:
            >>> # Filter tables with more than 1000 rows
            >>> large_tables = pt.filter_tables(lambda t: len(t.df) > 1000)
        """
        matching_tables = [table for table in self.tables if filter_func(table)]
        return PipelineTables(matching_tables)

    def get_table(self, name: str):
        """
        Retrieve a PipelineTable by its name.

        Args:
            name (str): The name of the table to retrieve.

        Returns:
            PipelineTable: The table with the specified name.

        Raises:
            KeyError: If no table with the specified name exists.

        Example:
            >>> table = pt_collection.get_table("my_table")
        """
        if name not in self.named_tables:
            raise KeyError(f"Table '{name}' not found")
        return self.named_tables[name]

    def __getitem__(self, name: str):
        """
        Allow dictionary-style access to tables by name.

        Args:
            name (str): The name of the table to retrieve.

        Returns:
            PipelineTable: The table with the specified name.

        Raises:
            KeyError: If no table with the specified name exists.

        Example:
            >>> table = pt_collection["my_table"]
        """
        return self.get_table(name)

    def __setitem__(self, name: str, table):
        """
        Allow dictionary-style assignment of tables by name.

        If a table with the same name already exists, it will be replaced.
        The table is added to both the tables list and the named_tables dictionary.

        Args:
            name (str): The name to assign to the table.
            table (PipelineTable): The PipelineTable instance to add.

        Raises:
            ValueError: If the table name is empty.
            TypeError: If table is not a PipelineTable instance.

        Example:
            >>> pt_collection["new_table"] = my_pipeline_table
        """
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
        """
        Allow dictionary-style deletion of table collections by name.

        Args:
            name (str): The name of the table to remove.

        Raises:
            KeyError: If no table with the specified name exists.

        Example:
            >>> del pt_collection["old_table"]
        """
        if name not in self.named_tables:
            raise KeyError(f"Table '{name}' not found")
        
        table = self.named_tables[name]
        if table in self.tables:
            self.tables.remove(table)
        del self.named_tables[name]

    def __contains__(self, name: str):
        """
        Check if a table with the given name exists in the collection.

        Args:
            name (str): The name to check for.

        Returns:
            bool: True if a table with the specified name exists, False otherwise.

        Example:
            >>> if "my_table" in pt_collection:
            >>>     print("Table exists!")
        """
        return name in self.named_tables

    def __len__(self):
        """
        Return the number of tables in the collection.

        Returns:
            int: The total number of tables in the collection.

        Example:
            >>> table_count = len(pt_collection)
        """
        return len(self.tables)

    @property
    def ntables(self):
        """
        Get the number of tables in the collection.
        
        This is a property that provides the same functionality as len(self),
        but with a more descriptive name for clarity.

        Returns:
            int: Number of tables in the collection.

        Example:
            >>> count = pt_collection.ntables
        """
        return len(self.tables)

    def save_events(self, table_names: list[str] = None):
        """
        Save events for all tables or specific tables in the collection.
        
        This method iterates through the specified tables and calls their save_events()
        method to persist the event logs to JSON files.

        Args:
            table_names (list[str], optional): List of table names to save events for.
                                             If None, saves events for all tables in the collection.
                                             Defaults to None.

        Returns:
            None

        Raises:
            KeyError: If any specified table name does not exist in the collection.
            Exception: If there are issues saving events for any table.

        Example:
            >>> # Save events for all tables
            >>> pt_collection.save_events()
            >>> 
            >>> # Save events for specific tables only
            >>> pt_collection.save_events(["table1", "table2"])
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
                
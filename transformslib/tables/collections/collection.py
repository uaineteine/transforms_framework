from transformslib.tables.metaframe import MetaFrame
import fnmatch
from typing import List
import os

class TableCollection:
    """
    A collection manager for multiple MetaFrame objects with dictionary-like access.
    
    This class provides a convenient way to manage multiple MetaFrame instances,
    allowing access by name through dictionary-style operations. It maintains both
    a list of tables and a dictionary for named access, ensuring consistency between
    the two data structures.
    
    Attributes:
        tables (list[MetaFrame]): List of all MetaFrame instances in the collection.
        named_tables (dict): Dictionary mapping table names to MetaFrame instances.
        
    Example:
        >>> # Create an empty collection
        >>> pt_collection = TableCollection()
        >>> 
        >>> # Add tables
        >>> pt1 = MetaFrame.load("data1.parquet", "parquet", "table1")
        >>> pt2 = MetaFrame.load("data2.parquet", "parquet", "table2")
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
    
    def __init__(self, tables: list[MetaFrame] = None):
        """
        Initialise a MetaFrames collection.

        Args:
            tables (list[MetaFrame], optional): Initial list of MetaFrame instances.
                                                   Defaults to None (empty collection).

        Example:
            >>> # Empty collection
            >>> pt_collection = MetaFrames()
            >>> 
            >>> # Collection with initial tables
            >>> tables = [pt1, pt2, pt3]
            >>> pt_collection = MetaFrames(tables)
        """
        self.tables = tables if tables is not None else []
        self.named_tables = {}        # Dict to access tables by name
        
        # Initialise named_tables if tables are provided
        if tables:
            for table in tables:
                if hasattr(table, 'table_name') and table.table_name:
                    self.named_tables[table.table_name] = table

        self.collection_version="1.0"

    def select_by_names(self, *name_patterns: str) -> 'TableCollection':
        """
        Select tables by name patterns, supporting wildcards and exact matches.
        
        This method creates a new TableCollection instance containing only the tables
        that match the specified name patterns. It supports:
        - Exact name matches: "table1", "table2"
        - Wildcard patterns: "clus_*", "*_2023", "table*"
        - Multiple patterns: "clus_*", "table1", "table3"
        
        Args:
            *name_patterns (str): Variable number of name patterns to match against.
                                Can be exact names or wildcard patterns.
        
        Returns:
            TableCollection: A new TableCollection instance containing only the matching tables.
        
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
        
        return TableCollection(matching_tables)

    def select_by_prefix(self, prefix: str) -> 'TableCollection':
        """
        Select tables that start with the specified prefix.
        
        This is a convenience method that uses select_by_names internally.
        
        Args:
            prefix (str): The prefix to match against table names.
        
        Returns:
            TableCollection: A new TableCollection instance containing only tables with matching prefix.
        
        Example:
            >>> # Select all tables starting with "clus_"
            >>> clus_tables = pt.select_by_prefix("clus_")
        """
        return self.select_by_names(f"{prefix}*")

    def select_by_suffix(self, suffix: str) -> 'TableCollection':
        """
        Select tables that end with the specified suffix.
        
        This is a convenience method that uses select_by_names internally.
        
        Args:
            suffix (str): The suffix to match against table names.
        
        Returns:
            MetaFrames: A new MetaFrames instance containing only tables with matching suffix.
        
        Example:
            >>> # Select all tables ending with "_2023"
            >>> tables_2023 = pt.select_by_suffix("_2023")
        """
        return self.select_by_names(f"*{suffix}")

    def select_by_range(self, start_name: str, end_name: str) -> 'TableCollection':
        """
        Select tables with names that fall within a lexicographic range.
        
        Args:
            start_name (str): The starting name (inclusive).
            end_name (str): The ending name (inclusive).
        
        Returns:
            MetaFrames: A new MetaFrames instance containing only tables within the range.
        
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
        
        return TableCollection(matching_tables)

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

    def filter_tables(self, filter_func) -> 'TableCollection':
        """
        Filter tables using a custom filter function.
        
        Args:
            filter_func (callable): A function that takes a MetaFrame and returns True/False.
        
        Returns:
            MetaFrames: A new MetaFrames instance containing only tables that pass the filter.
        
        Example:
            >>> # Filter tables with more than 1000 rows
            >>> large_tables = pt.filter_tables(lambda t: len(t.df) > 1000)
        """
        matching_tables = [table for table in self.tables if filter_func(table)]
        return TableCollection(matching_tables)

    def get_table(self, name: str):
        """
        Retrieve a MetaFrame by its name.

        Args:
            name (str): The name of the table to retrieve.

        Returns:
            MetaFrame: The table with the specified name.

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
            MetaFrame: The table with the specified name.

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
            table (MetaFrame): The MetaFrame instance to add.

        Raises:
            ValueError: If the table name is empty.
            TypeError: If table is not a MetaFrame instance.

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
                print(f"save events for table {table.table_name}")
                table.save_events()
        else:
            # Save events for specific tables
            for name in table_names:
                if name not in self.named_tables:
                    raise KeyError(f"Table '{name}' not found")
                self.named_tables[name].save_events()

    def save_all(self, output_dir:str, spark=None):
        """
        Save all tables in the collection to the specified output directory.
        
        This method iterates through all tables in the collection and calls their save()
        method to persist the data to files in the specified directory.

        Args:
            output_dir (str): The directory where all tables should be saved.

        Returns:
            None

        Raises:
            Exception: If there are issues saving any table.

        Example:
            >>> pt_collection.save_all("output_data/")
        """

        os.makedirs(output_dir, exist_ok=True)

        for table in self.tables:
            output_path = output_dir + "/" + table.table_name + ".parquet"
            table.write(path=output_path, spark=spark)
        
        self.save_events()
                
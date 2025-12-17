from transformslib.engine import get_engine, get_spark
from transformslib.templates.pathing import apply_formats
from multitable import MultiTable, FrameTypeVerifier
from transformslib.transforms.pipeevent import PipelineEvent
from naming_standards import Tablename
from transformslib import module_version

import pandas as pd
from tabulate import tabulate

import os
from typing import List

#structure type
class Meta:
    def __init__(self, inherit_events: List[PipelineEvent] = None):
        #store a version number
        self.meta_version = module_version
        #initialise events as a list, optionally inheriting from existing events
        self.events: List[PipelineEvent] = inherit_events.copy() if inherit_events else []

class MetaFrame(MultiTable):
    """
    A specialised class that extends multitable to include event logging capabilities for data pipeline operations.
    
    This class combines the functionality of a MultiTable (which handles different DataFrame types like PySpark, 
    Pandas, or Polars) with an event logging system that tracks all operations performed on the data.
    
    Attributes:
        meta
            events (List[PipelineEvent]): A list of events that have been logged during the pipeline operations.
            metaframe_version (str): Version identifier for the MetaFrame implementation.
            
    Example:
        >>> # Create a MetaFrame from an existing MultiTable
        >>> mf = MultiTable.load("data.parquet", "parquet", "my_table", "pyspark", spark)
        >>> pt = MetaFrame(mf)
        >>> 
        >>> # Add custom events
        >>> event = PipelineEvent("transform", "Applied filter", "Filtered rows where column > 10")
        >>> pt.add_event(event)
        >>> 
        >>> # Save all events to log files
        >>> pt.save_events()
    """

    def __init__(self, MultiTable: MultiTable, inherit_events: List[PipelineEvent] = None, person_keys:list[str]=None, warning_messages:pd.DataFrame = None, id_group_cd=None):
        """
        Initialise a MetaFrame with a MultiTable and optional event log.

        Args:
            MultiTable (MultiTable): A MultiTable object containing the DataFrame and metadata.
                                 Must be a valid MultiTable instance with df, src_path, 
                                 table_name, and frame_type attributes.
            inherit_events (List[PipelineEvent], optional): List of events to inherit from
                                 another MetaFrame. Defaults to None.
            person_keys (list[str], optional): List of person identifier keys. Defaults to empty list.
            warning_messages (pd.DataFrame, optional): DataFrame of warning messages. Defaults to None.
            id_group_cd (optional): Optional ID group code. Defaults to None.

        Raises:
            TypeError: If MultiTable is not a MultiTable instance.
            AttributeError: If MultiTable is missing required attributes.

        Example:
            >>> mf = MultiTable(df, "path/to/data.parquet", "my_table", "pyspark")
            >>> pt = MetaFrame(mf)
            >>> # With inherited events
            >>> pt_with_events = MetaFrame(mf, inherit_events=existing_events)
        """
        #call MultiTable constructor
        super().__init__(MultiTable.df, MultiTable.src_path, MultiTable.table_name, MultiTable.frame_type)

        self.meta = Meta(inherit_events=inherit_events)

        # Metadata for data quality and person tracking
        # Initialize attributes using setter methods for validation
        self.warning_messages = {}  # Initialize empty, then use setter
        self.person_keys = []  # Initialize empty, then use setter
        self.id_group_cd = None  # Initialize empty, then use setter
        
        # Use setters to apply validation
        if warning_messages:
            self.set_warning_messages(warning_messages)
        if person_keys:
            self.set_person_keys(person_keys)
        if id_group_cd:
            self.set_id_group_cd(id_group_cd)

        outpth = os.environ.get("TNSFRMS_LOG_LOC", "")
        outpth = apply_formats(outpth)
        dn = os.path.dirname(outpth)

        #debug path
        self.log_path = f"{dn}/debug_{self.table_name}_events.json"

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
        self.meta.events.append(event)

    def get_warning_messages(self):
        """
        Get the warning messages dictionary.
        
        Returns:
            dict: Mapping of column names to warning messages.
        
        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> warnings = pt.get_warning_messages()
        """
        return self.warning_messages
    
    def set_warning_messages(self, warning_messages: pd.DataFrame):
        """
        Set the warning messages table.
        
        Args:
            warning_messages (pd.DataFrame): Table of column names to warning messages.
        
        Raises:
            TypeError: If warning_messages is not a pd DataFrame.
        
        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> pt.set_warning_messages(pd.DataFrame({"col1": "Warning 1", "col2": "Warning 2"}))
        """
        if warning_messages is not None and not isinstance(warning_messages, pd.DataFrame):
            raise TypeError("MF502 warning_messages must be a pd DataFrame")
        def_cols = ["table_name", "column_name", "warning_messages", "processing_comments", "review_comments", "safe_data_comments"]
        self.warning_messages = warning_messages if warning_messages is not None else pd.DataFrame(columns=def_cols)
    
    def get_person_keys(self):
        """
        Get the list of person identifier keys.
        
        Returns:
            list: List of person identifier keys.
        
        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> keys = pt.get_person_keys()
        """
        return self.person_keys
    
    def set_person_keys(self, person_keys: list):
        """
        Set the list of person identifier keys.
        
        Args:
            person_keys (list): List of person identifier keys.
        
        Raises:
            TypeError: If person_keys is not a list.
        
        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> pt.set_person_keys(["person_id", "user_id"])
        """
        if person_keys is not None and not isinstance(person_keys, list):
            raise TypeError("MF501 person_keys must be a list of strings")
        self.person_keys = person_keys if person_keys is not None else []
    
    def get_id_group_cd(self):
        """
        Get the ID group code.
        
        Returns:
            Optional ID group code.
        
        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> id_group_cd = pt.get_id_group_cd()
        """
        return self.id_group_cd
    
    def set_id_group_cd(self, id_group_cd):
        """
        Set the ID group code.
        
        Args:
            id_group_cd: ID group code (can be any type).
        
        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> pt.set_id_group_cd("group_001")
        """
        self.id_group_cd = id_group_cd
    
    def get_metadata(self):
        """
        Return metadata information about the MetaFrame.
        
        Returns:
            dict: A dictionary containing warning_messages and person_keys.
                - warning_messages (pd.DataFrame): Mapping of column names to warning messages.
                - person_keys (list): List of person identifier keys.
        
        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> metadata = pt.get_metadata()
            >>> print(metadata['warning_messages'])
            >>> print(metadata['person_keys'])
        """
        return {
            'warning_messages': self.warning_messages,
            'person_keys': self.person_keys,
            'id_group_cd': self.id_group_cd
        }
    
    def info(self):
        """
        Print a formatted table showing column information including warnings and person key flags.
        
        Displays:
            - Table name and row count
            - For each column: name, person key indicator, and warning messages (if any)
        
        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> pt.info()
            Table: my_table (1000 rows)
            ================================================================================
            Column Name          | Person Key  | Warning Message
            ---------------------+-------------+--------------------------------------------
            name                 | *           | Contains PII
            age                  |             |
        """
        # Header
        print(f"\nTable: {self.table_name} ({self.nrow} rows) ID Group Code: {self.id_group_cd if self.id_group_cd else 'N/A'}")
        
        df_print = self.warning_messages.copy()
        nrow = df_print.shape[0]
        if nrow == 0:
            #get shape from person keys only
            df_print = pd.DataFrame({'column_name': self.columns})
        df_print['person_key'] = df_print['column_name'].apply(lambda col: '*' if col in self.person_keys else '')
        print(tabulate(df_print.values, df_print.columns, tablefmt="pretty", showindex=False))

    @staticmethod
    def load(path: str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, spark=None):
        """
        Load a DataFrame from the given path and return a MetaFrame with an initial load event.

        This static method creates a MetaFrame from a data file, automatically logging
        the load operation as the first event in the pipeline.

        Args:
            path (str): Path to the data file to load.
            format (str, optional): File format of the data. Defaults to "parquet". Supported formats: "parquet", "csv", "json", etc.
            table_name (str, optional): Name to assign to the table. Defaults to "".
            frame_type (str, optional): Type of DataFrame to create. Defaults to "pyspark". Supported types: "pyspark", "pandas", "polars".
            spark: SparkSession object (required for PySpark frame_type). Defaults to None.

        Returns:
            MetaFrame: A new MetaFrame instance with the loaded data and an initial load event.

        Raises:
            FileNotFoundError: If the specified path does not exist.
            ValueError: If the format or frame_type is not supported.
            Exception: If there are issues loading the data or creating the MultiTable.

        Example:
            >>> # Load a PySpark DataFrame
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table", "pyspark", spark)
            >>> 
            >>> # Load a Pandas DataFrame
            >>> pt = MetaFrame.load("data.csv", "csv", "my_table", "pandas")
        """
        #print(table_name)
        mf = MultiTable.load(path, format, table_name, frame_type, auto_lowercase=True, spark=spark)

        ptable = MetaFrame(mf)

        #construct payload to log
        payload = {
            "filepath": path,
            "table_name": table_name,
            "src_format": format
        }
        event = PipelineEvent("load", payload, event_description=f"Loaded {table_name} from {path}")
        ptable.add_event(event)
        
        return ptable

    def save_events(self, spark=None) -> None:
        """
        Save all logged events to a JSON file in the events_log directory.

        This method creates the events_log directory structure and saves each event
        as a separate JSON file. The events are organised by job and table name.

        Args:
            spark: SparkSession object (required for PySpark frame_type). Defaults to None.
            
        Returns:
            None

        Raises:
            OSError: If there are issues creating directories or writing files.
            Exception: If there are issues serializing events to JSON.

        Example:
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> pt.add_event(PipelineEvent("transform", "Filtered data", "Applied age > 18 filter"))
            >>> pt.save_events()  # Saves to events_log/job_1/my_table_events.json
        """
        dir = os.path.dirname(self.log_path)
        os.makedirs(dir, exist_ok=True)

        #spark flex
        spark = None
        if get_engine() == "pyspark":
            spark = get_spark()
        
        for event in self.meta.events:
            event.log_location = self.log_path
            event.log(spark=spark)
        
        print(f"Events saved to {self.log_path}")

    def copy(self, new_name: str = None):
        """
        Create a deep copy of the MetaFrame, including the DataFrame and all events.

        Returns:
            MetaFrame: A new MetaFrame instance with copied data and events.
        """
        
        copied_table = super().copy(new_name=new_name) #multitable copy
        
        # Copy events
        copied_events = self.meta.events.copy()
        return MetaFrame(copied_table, inherit_events=copied_events)

    def write(self, path: str, format: str = "parquet", overwrite: bool = True, spark=None):
        """
        Write the DataFrame to disk using MultiTable's write method and log the write event.

        Args:
            path (str): Destination path for the data.
            format (str, optional): File format to write. Defaults to "parquet".
            overwrite (bool, optional): Defaults to "TRUE".

        Returns:
            None

        Example:
            >>> pt.write("output.parquet", format="parquet")
        """

        #if data is modded, part the data
        try:
            id_mod_var = os.getenv("TNSFRMS_ID_MOD", "id_mod")
            if id_mod_var in self.columns:
                #rename variable to synth_id_mod
                dict_remap = { id_mod_var : "synth_id_mod"}
                self.rename(dict_remap)
        except Exception as e:
            print(f"MF900 Error in renaming id mod variable: {e}")

        #identify if set can be parted on write
        part_on = []
        try:
            if "synth_id_mod" in self.columns:
                part_on = ["synth_id_mod"]
        except Exception as e:
            print(f"MF901 Error in identifying sets for partition: {e}")

        # Write the data using MultiTable's write method 
        super().write(path, format=format, overwrite=overwrite, part_on=part_on, spark=spark)

        payload = {
            "filepath": path,
            "table_name": self.table_name,
            "out_format": format
        }

        # Log the write event
        event = PipelineEvent(
            event_type="write",
            event_payload=payload,
            event_description=f"Wrote table to {path} as {format} ({self.frame_type})"
        )
        event.filepath = path
        event.table_name = self.table_name
        event.src_format = format
        self.add_event(event)

    def sample(self, n: int = None, frac: float = None, seed: int = None):
        """
        Sample rows from the DataFrame and replace the existing DataFrame inplace.

        Args:
            n (int, optional): Number of rows to sample. Mutually exclusive with `frac`.
            frac (float, optional): Fraction of rows to sample. Mutually exclusive with `n`.
            seed (int, optional): Random seed for reproducibility.

        Returns:
            None
        """
        #run sample code from base
        super().sample(n=n, frac=frac, seed=seed)

        # Log event
        payload = {
            "n": n,
            "frac": frac,
            "seed": seed,
        }
        event = PipelineEvent(
            "transform",
            event_payload=payload,
            event_description="Sampled dataframe inplace"
        )
        self.add_event(event)

    def rename_table(self, new_name: str):
        """
        Rename the MultiTable's table name in place and log the event.

        Args:
            new_name (str): The new name to assign to the table.

        Returns:
            None

        Raises:
            ValueError: If the new_name is empty or invalid.
        """
        if not new_name or not isinstance(new_name, str):
            raise ValueError("Table name must be a non-empty string")

        old_name = str(self.table_name)
        self.table_name = Tablename(new_name)

        # Log the rename event with input_tables and output_tables
        payload = {
            "input_tables": [old_name],
            "output_tables": [new_name]
        }
        event = PipelineEvent(
            event_type="rename_table",
            event_payload=payload,
            event_description=f"Renamed table from {old_name} to {new_name}"
        )
        self.add_event(event)
        # No return; operation is inplace

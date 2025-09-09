from transformslib.tables.multitable import MultiTable
from transformslib.events.pipeevent import PipelineEvent
from transformslib import meta 

import os
from typing import List
from uainepydat.frameverifier import FrameTypeVerifier

#structure type
class Meta:
    def __init__(self, inherit_events: List[PipelineEvent] = None):
        #store a version number
        self.meta_version = meta.module_version
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

    def __init__(self, MultiTable: MultiTable, inherit_events: List[PipelineEvent] = None):
        """
        Initialise a MetaFrame with a MultiTable and optional event log.

        Args:
            MultiTable (MultiTable): A MultiTable object containing the DataFrame and metadata.
                                 Must be a valid MultiTable instance with df, src_path, 
                                 table_name, and frame_type attributes.
            inherit_events (List[PipelineEvent], optional): List of events to inherit from
                                 another MetaFrame. Defaults to None.

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
        self.log_path = f"events_log/job_1/table_specific/{self.table_name}_events.json"
    
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
        mf = MultiTable.load(path, format, table_name, frame_type, True, spark)

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
            >>> pt = MetaFrame.load("data.parquet", "parquet", "my_table")
            >>> pt.add_event(PipelineEvent("transform", "Filtered data", "Applied age > 18 filter"))
            >>> pt.save_events()  # Saves to events_log/job_1/my_table_events.json
        """
        dir = os.path.dirname(self.log_path)
        os.makedirs(dir, exist_ok=True)
        
        for event in self.meta.events:
            event.log_location = self.log_path
            event.log()
        
        print(f"Events saved to {self.log_path}")

    def copy(self, new_name: str = None):
        """
        Create a deep copy of the MetaFrame, including the DataFrame and all events.

        Returns:
            MetaFrame: A new MetaFrame instance with copied data and events.
        """
        if new_name is None:
            new_name = self.table_name

        # Copy the underlying MultiTable (data and metadata)
        copied_multitable = MultiTable(
            self.df.copy() if hasattr(self.df, "copy") else self.df,
            self.src_path,
            new_name,
            self.frame_type
        )
        # Copy events
        copied_events = self.meta.events.copy()
        return MetaFrame(copied_multitable, inherit_events=copied_events)

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
        # Write the data using MultiTable's write method
        super().write(path, format=format, overwrite=overwrite, spark=spark)

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

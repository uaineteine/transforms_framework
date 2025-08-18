import os
from typing import List

#module imports
from tables.metaframe import MetaFrame
from events.pipeline_event import PipelineEvent
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

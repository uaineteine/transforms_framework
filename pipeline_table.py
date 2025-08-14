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

        mf = MetaFrame.load(path, format, table_name, frame_type, spark)
        event = PipelineEvent(event_type="load", message=f"Loaded table from {path} as {format} ({frame_type})", description=f"Loaded {table_name} from {path}")

        ptable = PipelineTable(mf)
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

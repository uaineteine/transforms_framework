from transformslib.events.eventlog import Event
from transformslib import meta

class TransformEvent:
    def __init__(self, 
        input_tables: list[str],
        output_tables: list[str],
        input_variables: list[str],
        output_variables: list[str],
        created_variables: list[str] | None = None,
        renamed_variables: dict[str, str] | None = None,
        removed_variables: list[str] | None = None,
        input_row_counts: dict[str, int] | None = None,
        output_row_counts: dict[str, int] | None = None,
        input_columns: dict[str, list[str]] | None = None,
        output_columns: dict[str, list[str]] | None = None,
        ):

        self.input_tables = input_tables
        self.output_tables = output_tables
        self.input_variables = input_variables
        self.output_variables = output_variables
        self.created_variables = created_variables
        self.renamed_variables = renamed_variables
        self.removed_variables = removed_variables
        self.input_row_counts = input_row_counts
        self.output_row_counts = output_row_counts
        self.input_columns = input_columns
        self.output_columns = output_columns

         # derived attributes
        self.num_input_frames = len(input_tables)
        self.num_output_frames = len(output_tables)

class PipelineEvent(Event):
    """
    Specialised event class for pipeline operations.

    Extends the base Event class to include pipeline-specific metadata. Useful for
    logging data pipeline steps like loading, transformations, and saves.

    Attributes:
        event_type (str): The type of pipeline event (e.g., "load", "transform").
        event_payload (str): A short message or payload describing the operation.
        event_description (str): A detailed description of the operation.
        class_type (str): Constant string identifying this event type.
    """

    def __init__(self, event_type: str, event_payload: str, event_description: str = "", log_location: str = "", macro_uuid: str = None):
        """
        Create a PipelineEvent.

        Args:
            event_type (str): The type of pipeline event (e.g., "load", "transform", "save").
            event_payload (str): A short message or payload describing the operation.
            event_description (str, optional): A detailed description of the operation. Defaults to "".
            log_location (str, optional): Path to the log file. Defaults to "".
            macro_uuid (str, optional): UUID of the macro operation this event is part of. Defaults to None.

        Example:
            >>> event = PipelineEvent(
            ...     "load",
            ...     "Data loaded successfully",
            ...     "Loaded 1000 rows from data.parquet",
            ...     "logs/pipeline.log"
            ... )
            >>> print(event.event_payload)      # "Data loaded successfully"
            >>> print(event.event_description)  # "Loaded 1000 rows from data.parquet"
        """
        super().__init__(
            event_type=event_type,
            event_payload=event_payload,
            event_description=event_description,
            log_location=log_location,
            macro_uuid=macro_uuid
        )

        self.meta_version = meta.meta_version

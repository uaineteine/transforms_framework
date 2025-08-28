from transformslib.events.pipeevent import PipelineEvent
from transformslib.tables.collections.collection import TableCollection
from transformslib.tables.collections.supply_load import SupplyLoad
from transformslib.tables.names.lists import VarList
from transformslib.tables.names.headername import Headername
from transformslib.events.pipeevent import TransformEvent

import sys
import pyspark
import polars as pl
import pandas as pd

def_log_location = "events_log/job_1/transforms.json"
class Transform(PipelineEvent):
    """
    Base class for data transformation operations with automatic event logging.
    """

    def __init__(self, name: str, description: str, transform_type: str, testable_transform: bool = True):
        super().__init__("transform", None, event_description=description, log_location=def_log_location)
        self.name = name
        self.transform_type = transform_type
        self.testable_transform = testable_transform

        # Track version information
        self.version_pyspark = pyspark.__version__
        self.version_polars = pl.__version__
        self.version_pandas = pd.__version__
        self.version_python = sys.version.split()[0]
    
    def transforms(self, supply_frames: SupplyLoad, **kwargs) -> TableCollection:
        """
        Abstract method that must be implemented by subclasses.
        
        This method should contain the actual transformation logic for the data.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc. where
                     the keys are dataframe parameter names and values are table names in supply_frames.

        Returns:
            DataFrame: The transformed DataFrame.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        Example:
            >>> class MyTransform(Transform):
            ...     def transforms(self, supply_frames, **kwargs):
            ...         # Get dataframes by name
            ...         df1 = supply_frames[kwargs.get('df1')]
            ...         df2 = supply_frames[kwargs.get('df2')]
            ...         # Custom transformation logic
            ...         return df1.join(df2, on='id')
        """
        raise NotImplementedError("Subclasses should implement this method.")
    
    def error_check(self, supply_frames: SupplyLoad, **kwargs):
        """
        Abstract method for error checking before transformation.
        
        This method should contain validation logic to ensure the transformation
        can be safely applied to the provided data.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
            ValueError: If validation fails.

        Example:
            >>> class MyTransform(Transform):
            ...     def error_check(self, supply_frames, **kwargs):
            ...         # Validate that required columns exist
            ...         df = supply_frames[kwargs.get('df')]
            ...         if 'required_column' not in df.columns:
            ...             raise ValueError("Required column not found")
        """
        raise NotImplementedError("Subclasses should implement this method.")
    
    def test(self, supply_frames: SupplyLoad, **kwargs) -> bool:
        """
        Test method for validating transformation results.
        
        This method can be used to verify that the transformation was applied
        correctly and the results meet expected criteria.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc.

        Returns:
            bool: True if test passes, False otherwise.

        Example:
            >>> class MyTransform(Transform):
            ...     def test(self, supply_frames, **kwargs):
            ...         # Verify transformation results
            ...         df = supply_frames[kwargs.get('df')]
            ...         return len(df.columns) > 0
        """
        raise NotImplemented("Child classes to override this method")
        return True  # Default implementation always passes
    
    def __call__(self, supply_frames: SupplyLoad, **kwargs):
        """
        Call the transformation function with the provided supply frames and keyword arguments.
        
        This method provides a convenient callable interface for applying transformations.
        It automatically logs the transformation event after execution.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc.

        Returns:
            MetaFrame: The transformed MetaFrame.

        Example:
            >>> transform = MyTransform()
            >>> result = transform(supply_loader, df1="customers", df2="orders")  # Same as transform.apply(supply_loader, df1="customers", df2="orders")
        """
        return self.apply(supply_frames, **kwargs)
    
    def apply(self, supply_frames: SupplyLoad, **kwargs):
        # Perform error checking before transformation
        self.error_check(supply_frames, **kwargs)

        # Apply transformation
        result_df = self.transforms(supply_frames, **kwargs)

        # Perform testing after transformation
        if self.testable_transform:
            res = self.test(supply_frames, **kwargs)
            if not res:
                raise ValueError(f"Transform test failed for {self.name}")

        # Build TransformEvent payload for logging
        input_tables = [supply_frames[kwargs.get(k)].table_name for k in kwargs if k in supply_frames]
        output_tables = [result_df.table_name] if hasattr(result_df, "table_name") else []
        input_variables = [col.name for col in result_df.columns] if hasattr(result_df, "columns") else []
        output_variables = input_variables  # if same as input; adjust if different
        transform_event = TransformEvent(
            input_tables=input_tables,
            output_tables=output_tables,
            input_variables=input_variables,
            output_variables=output_variables,
            created_variables=getattr(result_df, "created_variables", None),
            renamed_variables=getattr(result_df, "renamed_variables", None),
            removed_variables=getattr(result_df, "deleted_variables", None),
        )

        # Store the TransformEvent as the payload
        self.event_payload = transform_event.__dict__

        # Log the event
        self.log()

        return result_df

class TableTransform(Transform):
    """
    Specialised transform class for operations that act on specific table variables.
    
    This class extends Transform to provide variable-level tracking and management
    for transformations that operate on specific columns or variables within a table.
    It maintains lists of target variables and tracks changes made during transformation.
    
    Attributes:
        target_variables (list[str]): List of variables that the transform operates on.
        target_tables (list): List of target tables (currently unused).
        created_variables (list): Variables created by the transformation.
        renamed_variables (list): Variables renamed by the transformation.
        deleted_variables (list): Variables deleted by the transformation.
        hashed_variables (list): Variables that were hashed during transformation.
        
    Example:
        >>> class ColumnFilter(TableTransform):
        ...     def __init__(self, columns):
        ...         super().__init__("ColumnFilter", "Filter specific columns", columns)
        ...     
        ...     def transforms(self, supply_frames, **kwargs):
        ...         df = supply_frames[kwargs.get('df')]
        ...         return df.select(self.target_variables)
        >>> 
        >>> filter_transform = ColumnFilter(["col1", "col2"])
        >>> result = filter_transform(supply_loader, df="table_name")
    """

    def update_target_variables(self, acts_on_variables: str | list[str] | None):
        if acts_on_variables is None:
            self.target_variables = []
            return

        if isinstance(acts_on_variables, list) and len(acts_on_variables) == 1 and isinstance(acts_on_variables[0], str):
            # Unwrap single-element lists
            acts_on_variables = acts_on_variables[0]

        if isinstance(acts_on_variables, str):
            self.target_variables = [Headername(acts_on_variables)]
        elif isinstance(acts_on_variables, list) and all(isinstance(v, str) for v in acts_on_variables):
            self.target_variables = [Headername(var) for var in acts_on_variables]
        else:
            raise ValueError("acts_on_variables must be a string, a list of strings, or None")

    def __init__(
        self,
        name: str,
        description: str,
        acts_on_variables: str | list[str] | None,
        transform_id: str,
        testable_transform: bool = False
    ):
        """
        Initialise a TableTransform with target variables.

        Args:
            name (str): The name of the transformation.
            description (str): Description of what the transformation does.
            acts_on_variables (str | list[str] | None): Variable name(s) that the transform operates on.
                Can be None if the transform does not act on specific variables.
            transform_id (str): Unique identifier for the transform.
            testable_transform (bool): Whether this transform can be tested. Defaults to False.

        Raises:
            ValueError: If transform_id is blank.

        Example:
            >>> TableTransform("ColumnSelect", "Select column", "col1", "transform_001")
            >>> TableTransform("ColumnSelect", "Select columns", ["col1", "col2"], "transform_002")
            >>> TableTransform("DistinctRows", "Remove duplicates", None, "transform_003")
        """
        super().__init__(name, description, "TableTransform", testable_transform=testable_transform)

        if not transform_id:
            raise ValueError("Transform ID must be non-blank")
        self.transform_id = transform_id

        self.target_tables = []
        self.update_target_variables(acts_on_variables)
        
        # Validate target variables using VarList
        try:
            self.target_variables = VarList(self.target_variables)
        except ValueError as e:
            raise ValueError(f"Invalid header names: {e}")

        # Initialise variable lists
        self.created_variables = None
        self.renamed_variables = None
        self.deleted_variables = None
        self.hashed_variables = None

    @property
    def nvars(self):
        """Returns the number of target variables."""
        return len(self.target_variables)

    @property
    def vars(self):
        """
        Returns:
        list[str]: Single variable if one, list if multiple.
        """
        return self.target_variables

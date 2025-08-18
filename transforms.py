import json
from pipeline_event import PipelineEvent
from pipeline_table import PipelineTables
from supply_load import SupplyLoad

class Transform(PipelineEvent):
    """
    Base class for data transformation operations with automatic event logging.
    
    This class provides the foundation for implementing data transformations in the pipeline.
    It extends PipelineEvent to automatically log transformation operations and provides
    a consistent interface for applying transformations to MetaFrame objects.
    
    Attributes:
        name (str): The name of the transformation.
        transform_type (str): The type/category of the transformation.
        
    Example:
        >>> class MyTransform(Transform):
        ...     def __init__(self):
        ...         super().__init__("MyTransform", "Custom transformation", "custom")
        ...     
        ...     def transforms(self, supply_frames, **kwargs):
        ...         # Implementation here
        ...         return transformed_df
        >>> 
        >>> transform = MyTransform()
        >>> result = transform(supply_loader, df1="customers", df2="orders")  # Automatically logs the transformation
    """

    def __init__(self, name: str, description: str, transform_type: str, testable_transform: bool = True):
        """
        Initialize a Transform with name, description, and type.

        Args:
            name (str): The name of the transformation operation.
            description (str): A detailed description of what the transformation does.
            transform_type (str): The category or type of transformation.
            testable_transform (bool): Whether this transform can be tested. Defaults to True.

        Example:
            >>> transform = Transform("DataClean", "Remove null values", "cleaning")
            >>> print(transform.name)  # "DataClean"
            >>> print(transform.transform_type)  # "cleaning"
        """
        super().__init__(event_type="transform", message=name, description=description, log_location="events_log/job_1/transforms.json")
        self.name = name  # Set name manually
        self.transform_type = transform_type
        self.testable_transform = testable_transform
    
    def transforms(self, supply_frames: SupplyLoad, **kwargs) -> PipelineTables:
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
        """
        Apply the transformation to the provided supply frames with keyword arguments.
        
        This method executes the transformation and automatically logs the operation
        as a pipeline event.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc.

        Returns:
            MetaFrame: The transformed MetaFrame.

        Example:
            >>> transform = MyTransform()
            >>> result = transform.apply(supply_loader, df1="customers", df2="orders")
            >>> # Transformation is automatically logged
        """
        #Apply transformation
        result_df = self.transforms(supply_frames, **kwargs)

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

    def __init__(self, name: str, description: str, acts_on_variables: list[str], testable_transform: bool = False):
        """
        Initialize a TableTransform with target variables.

        Args:
            name (str): The name of the transformation.
            description (str): Description of what the transformation does.
            acts_on_variables (list[str]): List of variable names that the transform operates on.
            testable_transform (bool): Whether this transform can be tested. Defaults to False.

        Raises:
            ValueError: If no target variables are provided.

        Example:
            >>> transform = TableTransform("ColumnSelect", "Select specific columns", ["col1", "col2"])
            >>> print(transform.target_variables)  # ["col1", "col2"]
        """
        super().__init__(name, description, "TableTransform", testable_transform=testable_transform)

        self.target_tables = [] #nadah to begin with

        self.target_variables = acts_on_variables
        if len(acts_on_variables) == 0:
            raise ValueError("No target variables defined for this transform.")
        
        #initalise variable lists
        self.created_variables = None
        self.renamed_variables = None
        self.deleted_variables = None
        self.hashed_variables = None

    
    @property
    def nvars(self):
        """
        Get the number of target variables for this transform.

        Returns:
            int: Number of target variables.

        Example:
            >>> transform = TableTransform("MyTransform", "Description", ["col1", "col2", "col3"])
            >>> print(transform.nvars)  # 3
        """
        return len(self.target_variables)
    
    @property
    def vars(self):
        """
        Get the target variable(s) for this transform.
        
        Returns a single variable if there's only one, or the full list if there are multiple.

        Returns:
            str or list[str]: The target variable(s).

        Raises:
            ValueError: If no target variables are defined.

        Example:
            >>> # Single variable
            >>> transform = TableTransform("MyTransform", "Description", ["col1"])
            >>> print(transform.vars)  # "col1"
            >>> 
            >>> # Multiple variables
            >>> transform = TableTransform("MyTransform", "Description", ["col1", "col2"])
            >>> print(transform.vars)  # ["col1", "col2"]
        """
        if self.nvars > 1:
            return self.target_variables
        elif self.nvars == 1:
            return self.target_variables[0]
        else:
            raise ValueError("No target variables defined for this transform.")

class SimpleTransform(TableTransform):
    """
    Simplified transform class for operations that act on a single variable.
    
    This class provides a convenient wrapper for TableTransform when working with
    single-variable operations, automatically wrapping the variable in a list.

    Example:
        >>> class DropColumn(SimpleTransform):
        ...     def __init__(self, column_name):
        ...         super().__init__("DropColumn", f"Drop column {column_name}", column_name)
        ...     
        ...     def transforms(self, supply_frames, **kwargs):
        ...         df = supply_frames[kwargs.get('df')]
        ...         return df.drop(self.var)
        >>> 
        >>> drop_transform = DropColumn("unwanted_column")
        >>> result = drop_transform(supply_loader, df="table_name")
    """

    def __init__(self, name: str, description: str, acts_on_variable: str, testable_transform: bool = False):
        """
        Initialize a SimpleTransform with a single target variable.

        Args:
            name (str): The name of the transformation.
            description (str): Description of what the transformation does.
            acts_on_variable (str): The single variable that the transform operates on.
            testable_transform (bool): Whether this transform can be tested. Defaults to False.

        Example:
            >>> transform = SimpleTransform("MyTransform", "Description", "column_name")
            >>> print(transform.var)  # "column_name"
        """
        super().__init__(name, description, [acts_on_variable], testable_transform=testable_transform)
    
    @property
    def var(self):
        """
        Get the single target variable for this transform.

        Returns:
            str: The target variable.

        Example:
            >>> transform = SimpleTransform("MyTransform", "Description", "column_name")
            >>> print(transform.var)  # "column_name"
        """
        return self.target_variables[0]

class DropVariable(SimpleTransform):
    """
    Transform class for removing variables/columns from a DataFrame.
    
    This class provides a specific implementation for dropping columns from a DataFrame.
    It automatically validates that the target variable exists before attempting to drop it.

    Example:
        >>> drop_transform = DropVariable("unwanted_column")
        >>> result = drop_transform(supply_loader, df="table_name")
        >>> # The column is removed and the operation is logged
    """

    def __init__(self, variable_to_drop: str):
        """
        Initialize a DropVariable transform.

        Args:
            variable_to_drop (str): The name of the variable/column to remove from the DataFrame.

        Example:
            >>> drop_transform = DropVariable("old_column")
            >>> print(drop_transform.name)  # "DropVariable"
            >>> print(drop_transform.var)  # "old_column"
        """
        #REPLACE HERE WITH YOUR OWN MESSAGE
        super().__init__("DropVariable", "Removes this variable from a dataframe", variable_to_drop)

    def transforms(self, supply_frames: SupplyLoad, **kwargs):
        """
        Remove the specified variable from the DataFrame.
        
        This method validates that the target variable exists in the DataFrame,
        removes it, and updates the tracking information.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc.

        Returns:
            MetaFrame: The MetaFrame with the variable removed.

        Raises:
            ValueError: If the target variable is not found in the DataFrame columns.
            KeyError: If the specified table name is not found in supply_frames.

        Example:
            >>> drop_transform = DropVariable("unwanted_column")
            >>> result = drop_transform.transforms(supply_loader, df="table_name")
            >>> # The column is removed from the specified table
        """
        # Get the table from supply_frames
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        
        #PUT HERE ERROR CHECKING
        if self.var not in supply_frames[table_name].columns:
            raise ValueError(f"Variable '{self.var}' not found in DataFrame columns: {supply_frames[table_name].columns}")

        #PUT HERE TRANSFORMATION LOGIC
        self.deleted_variables = [self.var]
        self.target_tables = [table_name]
        supply_frames[table_name].df = supply_frames[table_name].df.drop(self.var)

        supply_frames[table_name].events.append(self)
        return supply_frames

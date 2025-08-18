from transforms import Transform, SimpleTransform
from supply_load import SupplyLoad

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
        super().__init__("DropVariable", "Removes this variable from a dataframe", variable_to_drop, testable_transform=True)

    def error_check(self, supply_frames: SupplyLoad, **kwargs):
        """
        Validate that the variable to drop exists in the DataFrame.
        
        This method checks that the target variable exists in the specified table
        before attempting to drop it.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc.

        Raises:
            ValueError: If the target variable is not found in the DataFrame columns.
            KeyError: If the specified table name is not found in supply_frames.

        Example:
            >>> drop_transform = DropVariable("unwanted_column")
            >>> drop_transform.error_check(supply_loader, df="table_name")
        """
        # Get the table from supply_frames
        table_name = kwargs.get('df')
        if not table_name:
            raise ValueError("Must specify 'df' parameter with table name")
        
        # Check if the variable exists in the DataFrame
        if self.var not in supply_frames[table_name].columns:
            raise ValueError(f"Variable '{self.var}' not found in DataFrame columns: {supply_frames[table_name].columns}")

    def transforms(self, supply_frames: SupplyLoad, **kwargs):
        """
        Remove the specified variable from the DataFrame.
        
        This method removes the target variable and updates the tracking information.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc.

        Returns:
            MetaFrame: The MetaFrame with the variable removed.

        Example:
            >>> drop_transform = DropVariable("unwanted_column")
            >>> result = drop_transform.transforms(supply_loader, df="table_name")
            >>> # The column is removed from the specified table
        """
        # Get the table from supply_frames
        table_name = kwargs.get('df')
        
        # Apply transformation logic
        self.deleted_variables = [self.var]
        self.target_tables = [table_name]
        supply_frames[table_name].df = supply_frames[table_name].df.drop(self.var)

        supply_frames[table_name].events.append(self)
        return supply_frames
    
    def test(self, supply_frames: SupplyLoad, **kwargs) -> bool:
        """
        Test that the variable was successfully removed from the DataFrame.
        
        This method verifies that the target variable no longer exists in the
        specified table after the transformation.

        Args:
            supply_frames (SupplyLoad): The supply frames collection containing the dataframes.
            **kwargs: Keyword arguments in the format df1="name1", df2="name2" etc.

        Returns:
            bool: True if the variable was successfully removed, False otherwise.

        Example:
            >>> drop_transform = DropVariable("unwanted_column")
            >>> result = drop_transform.test(supply_loader, df="table_name")
        """
        table_name = kwargs.get('df')
        if not table_name:
            return False
        
        # Check that the variable is no longer in the DataFrame
        return self.var not in supply_frames[table_name].columns

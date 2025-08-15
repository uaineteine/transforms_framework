import re

class Tablename(str):
    """
    A specialized string class for validating and managing table names.
    
    This class extends the built-in str class to provide validation and formatting
    for table names. It ensures that table names follow proper naming conventions
    and provides a clean interface for working with table identifiers.
    
    The class validates that table names:
    - Start with a letter or underscore
    - Contain only alphanumeric characters and underscores
    - Are not empty
    - Can be purely numeric (special case)
    
    Example:
        >>> # Valid table names
        >>> name1 = Tablename("my_table")
        >>> name2 = Tablename("_private_table")
        >>> name3 = Tablename("123")  # Numeric names are allowed
        >>> 
        >>> # Invalid table names (will raise ValueError)
        >>> # Tablename("1table")  # Starts with number
        >>> # Tablename("table-name")  # Contains hyphen
        >>> # Tablename("")  # Empty string
    """

    @staticmethod
    def acceptable_format(name: str) -> bool:
        """
        Check if a table name follows acceptable naming conventions.
        
        This static method validates table names using a regular expression pattern.
        It allows names that start with a letter or underscore, followed by any
        combination of alphanumeric characters and underscores. Purely numeric
        names are also accepted as a special case.

        Args:
            name (str): The table name to validate.

        Returns:
            bool: True if the name follows acceptable format, False otherwise.

        Example:
            >>> Tablename.acceptable_format("my_table")  # True
            >>> Tablename.acceptable_format("_private")  # True
            >>> Tablename.acceptable_format("123")       # True
            >>> Tablename.acceptable_format("1table")    # False
            >>> Tablename.acceptable_format("table-name") # False
            >>> Tablename.acceptable_format("")          # False
        """
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
        
        if not name.isdigit():
            if not re.match(pattern, name):
                return False

        return True

    def __init__(self, table_name: str):
        """
        Initialize a Tablename with validation.
        
        This constructor validates the provided table name and raises an error
        if it doesn't meet the naming requirements. The validation ensures
        database compatibility and prevents naming conflicts.

        Args:
            table_name (str): The table name to validate and store.

        Raises:
            ValueError: If the table name is empty.
            ValueError: If the table name doesn't follow acceptable format.

        Example:
            >>> # Valid initialization
            >>> name = Tablename("customer_data")
            >>> print(name)  # "customer_data"
            >>> 
            >>> # Invalid initialization (will raise ValueError)
            >>> # name = Tablename("")  # Empty name
            >>> # name = Tablename("1invalid")  # Starts with number
        """
        if table_name == "":
            raise ValueError("Table name cannot be empty")
        
        #ensure format of table name is correct
        #based on regex
        #no starting numbers unless it is only numbers, no special characters except underscore
        if Tablename.acceptable_format(table_name) == False:
            raise ValueError(f"Invalid table name format: {table_name}. Must start with a letter or underscore, followed by alphanumeric characters or underscores.")
        
        self = table_name

import re

class Tablename(str):

    @staticmethod
    def acceptable_format(name: str) -> bool:
        """
        Regular expression to validate table names.
        Must start with a letter or underscore, followed by alphanumeric characters or underscores.
        Example of valid names: 'table1', '_table_name', 'TableName123'.
        Example of invalid names: '1table', 'table-name!', 'table name'.

        :return: Regular expression string for acceptable table names.
        """
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
        
        if not name.isdigit():
            if not re.match(pattern, name):
                return False

        return True

    """
    Class to handle the table name with error checking
    """
    def __init__(self, table_name: str):
        """
        Initialize the Tablename with a string.
        :param table_name: Name of the table.
        """
        if table_name == "":
            raise ValueError("Table name cannot be empty")
        
        #ensure format of table name is correct
        #based on regex
        #no starting numbers unless it is only numbers, no special characters except underscore
        if Tablename.acceptable_format(table_name) == False:
            raise ValueError(f"Invalid table name format: {table_name}. Must start with a letter or underscore, followed by alphanumeric characters or underscores.")
        
        self = table_name

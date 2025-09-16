import re

class Tablename(str):
    """
    A specialised string class for validating and managing table names.

    This class extends str to enforce naming conventions for database table identifiers.

    Valid table names:
    - Must not be empty
    - Can be purely numeric (e.g., "123")
    - Otherwise, must start with a letter or underscore
    - Must contain only alphanumeric characters and underscores

    Example:
        >>> Tablename("my_table")        # Valid
        >>> Tablename("_private_table")  # Valid
        >>> Tablename("123")             # Valid
        >>> Tablename("1table")          # Invalid
        >>> Tablename("table-name")      # Invalid
        >>> Tablename("")                # Invalid
    """

    @staticmethod
    def acceptable_format(name: str) -> bool:
        """
        Validate the format of a table name.

        Args:
            name (str): The table name to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        if not name:
            return False
        if name.isdigit():
            return True
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
        return bool(re.match(pattern, name))

    def __new__(cls, table_name: str):
        """
        Create a new Tablename instance with validation.

        Args:
            table_name (str): The table name to validate.

        Raises:
            ValueError: If the name is empty or improperly formatted.
        """
        if not cls.acceptable_format(table_name):
            raise ValueError(
                f"Invalid table name format: '{table_name}'. "
                "Must be non-empty, start with a letter or underscore, and contain only alphanumeric characters or underscores. "
                "Purely numeric names are allowed."
            )
        return str.__new__(cls, table_name)

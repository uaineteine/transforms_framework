import re

class Colname(str):
    """
    A specialised string class for validating and managing column names.

    This class ensures header names:
    - Are not empty
    - Contain only lowercase letters, digits, and underscores
    - Have no spaces or other special characters

    Example:
        >>> Colname("customer_name")  # Valid
        >>> Colname("order_123")      # Valid
        >>> Colname("CustomerName")   # Invalid (not all lowercase)
        >>> Colname("ORDER DATE")     # Invalid (contains space)
        >>> Colname("")               # Invalid (empty)
    """

    @staticmethod
    def acceptable_format(name: str) -> bool:
        """
        Validate header name format.

        Args:
            name (str): The header name to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        pattern = r"^[a-z0-9_]+$"
        return bool(re.match(pattern, name))

    def __new__(cls, header_name: str):
        if not header_name:
            raise ValueError("Header name cannot be empty.")

        header_name = header_name.lower()  # normalize

        if not cls.acceptable_format(header_name):
            raise ValueError(
                f"Invalid header name format: '{header_name}'. "
                "Headers must be all lowercase and contain only letters, digits, and underscores—no spaces or other symbols."
            )
        return str.__new__(cls, header_name)

import re

class Headername(str):
    """
    A specialised string class for validating and managing header names.

    This class ensures header names:
    - Are not empty
    - Contain only uppercase letters, digits, and underscores
    - Have no spaces or other special characters

    Example:
        >>> Headername("CUSTOMER_NAME")  # Valid
        >>> Headername("ORDER_123")      # Valid
        >>> Headername("CustomerName")   # Invalid (not all caps)
        >>> Headername("ORDER DATE")     # Invalid (contains space)
        >>> Headername("")               # Invalid (empty)
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
        pattern = r"^[A-Z0-9_]+$"
        return bool(re.match(pattern, name))

    def __new__(cls, header_name: str):
        if not header_name:
            raise ValueError("Header name cannot be empty.")

        header_name = header_name.upper()  # normalize

        if not cls.acceptable_format(header_name):
            raise ValueError(
                f"Invalid header name format: '{header_name}'. "
                "Headers must be ALL CAPS and contain only letters, digits, and underscores—no spaces or other symbols."
            )
        return str.__new__(cls, header_name)

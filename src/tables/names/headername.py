import re

class Headername(str):
    """
    A specialised string class for validating and managing header names.

    This class ensures header names:
    - Are not empty
    - Contain only uppercase letters and digits
    - Have no spaces or special characters

    Example:
        >>> Headername("CUSTOMERNAME")  # Valid
        >>> Headername("ORDER123")      # Valid
        >>> Headername("CustomerName")  # Invalid (not all caps)
        >>> Headername("ORDER DATE")    # Invalid (contains space)
        >>> Headername("")              # Invalid (empty)
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
        pattern = r"^[A-Z0-9]+$"
        return bool(re.match(pattern, name))

    def __new__(cls, header_name: str):
        """
        Create a new Headername instance with validation.

        Args:
            header_name (str): The header name to validate.

        Raises:
            ValueError: If the header name is empty or improperly formatted.
        """
        if not cls.acceptable_format(header_name):
            raise ValueError(
                f"Invalid header name format: '{header_name}'. "
                "Headers must be ALL CAPS and contain only letters and digits—no spaces or symbols."
            )
        return str.__new__(cls, header_name)

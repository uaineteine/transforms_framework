import json

from .colname import Colname

def auto_lowercase_list(list_of_strings: list[str]) -> list[str]:
    """
    Convert all strings in the input list to lowercase.

    :param list_of_strings: List of strings to convert.
    :type list_of_strings: list[str]
    :return: List of lowercased strings.
    :rtype: list[str]
    """
    return [var.lower() for var in list_of_strings]

def auto_capitalise_list(list_of_strings: list[str]) -> list[str]:
    """
    Convert all strings in the input list to uppercase.

    :param list_of_strings: List of strings to convert.
    :type list_of_strings: list[str]
    :return: List of uppercased strings.
    :rtype: list[str]
    """
    return [var.upper() for var in list_of_strings]

class NamedList(list[str]):
    """
    A list of strings with enforced lowercase and utility methods for name management.
    """
    def __init__(self, items: list[str]):
        """
        Initialize a NamedList with all items converted to lowercase.

        :param items: List of string items.
        :type items: list[str]
        :raises TypeError: If any item is not a string.
        """
        if not all(isinstance(item, str) for item in items):
            raise TypeError("All items must be strings.")
        lowercased = auto_lowercase_list(items)
        super().__init__()
        self.extend(lowercased)
    
    def __repr__(self):
        """
        Return a string representation of the NamedList.

        :return: String representation.
        :rtype: str
        """
        return f"NamedList({list(self)})"
    
    @property
    def count(self) -> int:
        """
        Get the number of items in the NamedList.

        :return: Number of items.
        :rtype: int
        """
        return len(self)
    
    def to_json(self) -> str:
        """
        Serialize the NamedList to a JSON string.

        :return: JSON string with variable names.
        :rtype: str
        """
        return json.dumps({"var_names": self}, indent=2)

    def overlap(self, other: list[str]) -> list[str]:
        """
        Return a NamedList of items that overlap with another list.

        :param other: List of strings to compare.
        :type other: list[str]
        :return: NamedList of overlapping items.
        :rtype: NamedList
        """
        return NamedList(list(set(self) & set(other)))

    def extend_with(self, other: list[str]) -> 'NamedList':
        """
        Extend the NamedList with unique items from another list.

        :param other: List of strings to add.
        :type other: list[str]
        :return: The updated NamedList instance.
        :rtype: NamedList
        """
        combined = list(set(self) | set(other))
        self.clear()
        self.extend(combined)
        return self

class ColList(NamedList):
    """
    A NamedList with additional validation for acceptable column name formats.
    """
    def __init__(self, items: list[str]):
        """
        Initialize a ColList and check that all items are in an acceptable format.

        :param items: List of string items.
        :type items: list[str]
        :raises TypeError: If any item is not a string.
        :raises ValueError: If any item is not in an acceptable format.
        """
        if not all(isinstance(item, str) for item in items):
            raise TypeError("All items must be strings.")
        super().__init__(items)
        self.acceptable_format_check()

    def acceptable_format(self) -> bool:
        """
        Check if all items in the ColList are in an acceptable column name format.

        :return: True if all items are acceptable, False otherwise.
        :rtype: bool
        """
        for str_item in self:
            check = Colname.acceptable_format(str_item)
            if check == False:
                return False
        return True

    def acceptable_format_check(self):
        """
        Raise ValueError if any item in the ColList is not in an acceptable format.

        :raises ValueError: If any item is not in an acceptable format.
        """
        check = self.acceptable_format()
        if check == False:
            raise ValueError("Column names must be in correct format")

# if __name__ == "__main__":
#     # Basic valid input
#     a = ColList(["foo", "bar", "baz"])
#     print("List A:", a)

#     # Input with pure numbers and spaces
#     try:
#         b = ColList(["123", "hello world", "BAR"])
#         print("List B:", b)
#     except ValueError as e:
#         print("Caught ValueError for List B:", e)

#     # Overlap test
#     c = ColList(["bar", "qux", "456"])
#     print("Overlap A & C:", a.overlap(c))

#     # Extend test
#     a.extend_with(c)
#     print("Extended A:", a)

#     # JSON output
#     print("JSON A:", a.to_json())

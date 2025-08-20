import json

from tables.names.headername import Headername

def auto_capitalise_list(list_of_strings: list[str]) -> list[str]:
    return [var.upper() for var in list_of_strings]

class NamedList(list[str]):
    def __init__(self, items: list[str]):
        if not all(isinstance(item, str) for item in items):
            raise TypeError("All items must be strings.")
        capitalised = auto_capitalise_list(items)
        super().__init__()
        self.extend(capitalised)
    
    def __repr__(self):
        return f"NamedList({list(self)})"
    
    @property
    def count(self) -> int:
        return len(self)
    
    def to_json(self) -> str:
        return json.dumps({"var_names": self}, indent=2)

    def overlap(self, other: list[str]) -> list[str]:
        return NamedList(list(set(self) & set(other)))

    def extend_with(self, other:list[str]) -> None:
        combined = list(set(self) | set(other))
        self.clear()
        self.extend(combined)

class VarList(NamedList):
    def __init__(self, items: list[str]):
        if not all(isinstance(item, str) for item in items):
            raise TypeError("All items must be strings.")
        
        super().__init__(items)

        self.acceptable_format_check()

    def acceptable_format(self) -> bool:
        for str_item in self:
            check = Headername.acceptable_format(str_item)
            if check == False:
                return False
        #implied else
        return True

    def acceptable_format_check(self):
        check = self.acceptable_format()
        if check == False:
            raise ValueError("Column names must be in correct format")

if __name__ == "__main__":
    # Basic valid input
    a = VarList(["foo", "bar", "baz"])
    print("List A:", a)

    # Input with pure numbers and spaces
    try:
        b = VarList(["123", "hello world", "BAR"])
        print("List B:", b)
    except ValueError as e:
        print("Caught ValueError for List B:", e)

    # Overlap test
    c = VarList(["bar", "qux", "456"])
    print("Overlap A & C:", a.overlap(c))

    # Extend test
    a.extend_with(c)
    print("Extended A:", a)

    # JSON output
    print("JSON A:", a.to_json())

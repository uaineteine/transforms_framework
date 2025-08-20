import json

def auto_capitalise_list(list_of_strings: list[str]) -> list[str]:
    return [var.upper() for var in list_of_strings]

class VarList(list[str]):
    def __init__(self, items: list[str]):
        if not all(isinstance(item, str) for item in items):
            raise TypeError("All items must be strings.")
        capitalised = auto_capitalise_list(items)
        super().__init__()
        self.extend(capitalised)

    def __repr__(self):
        return f"VarList({list(self)})"

    @property
    def count(self) -> int:
        return len(self)

    def to_json(self) -> str:
        return json.dumps({"var_names": self}, indent=2)

    def overlap(self, other: "VarList") -> "VarList":
        return VarList(list(set(self) & set(other)))

    def extend_with(self, other: "VarList") -> None:
        combined = list(set(self) | set(other))
        self.clear()
        self.extend(combined)

if __name__ == "__main__":
    a = VarList(["foo", "bar", "baz"])
    b = VarList(["BAR", "qux"])

    print("List A:", a)                  # VarList(['FOO', 'BAR', 'BAZ'])
    print("List B:", b)                  # VarList(['BAR', 'QUX'])
    print("Count A:", a.count)           # 3
    print("JSON A:", a.to_json())        # JSON string
    print("Overlap A & B:", a.overlap(b))# VarList(['BAR'])

    a.extend_with(b)
    print("Extended A:", a)              # VarList(['FOO', 'BAR', 'BAZ', 'QUX'])

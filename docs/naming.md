# Naming

**NOTE: As of version 0.13.0, the naming functionality has been moved to a separate package.**

The naming functionality is now available as a standalone package `naming-standards` (v1.0.0). Install it separately:

```bash
pip install naming-standards
```

Usage:
```python
from naming_standards import Tablename, Colname, ColList, NamedList
```

The naming-standards package provides standardised classes for validating and managing table names, column headers, and variable lists. These utilities ensure consistent naming conventions across your data pipeline.

### 1. Tablename
**Purpose:** Validates and standardises table names according to database naming conventions.

**Features:**
- Enforces valid database table identifier rules
- Allows purely numeric names (e.g., "123")
- Requires non-numeric names to start with a letter or underscore
- Permits only alphanumeric characters and underscores
- Extends the built-in `str` class for seamless integration

**Validation Rules:**
- Must not be empty
- Can be purely numeric (e.g., "123")
- Otherwise, must start with a letter (a-z, A-Z) or underscore (_)
- Must contain only alphanumeric characters and underscores
- No spaces or special characters (except underscore)

**Examples:**
```python
from naming_standards import Tablename

# Valid table names
valid_names = [
    Tablename("my_table"),        # Standard format
    Tablename("_private_table"),  # Starting with underscore
    Tablename("123"),             # Purely numeric
    Tablename("table123"),        # Mixed alphanumeric
    Tablename("CustomerData"),    # CamelCase
]

# Invalid table names (will raise ValueError)
try:
    Tablename("1table")          # Cannot start with digit
except ValueError as e:
    print(e)

try:
    Tablename("table-name")      # No hyphens allowed
except ValueError as e:
    print(e)

try:
    Tablename("")                # Cannot be empty
except ValueError as e:
    print(e)
```

### 2. Colname
**Purpose:** Validates and standardises column header names with strict formatting rules.

**Features:**
- Enforces lowercase-only column headers
- Automatically converts input to lowercase
- Allows only letters and digits
- No spaces, symbols, or special characters permitted
- Underscores are allowed
- Extends the built-in `str` class for seamless integration

**Validation Rules:**
- Must not be empty
- Must contain only lowercase letters (a-z) and digits (0-9)
- No spaces or special characters except underscores
- Underscores are allowed
- Input is automatically converted to lowercase

**Examples:**
```python
from transformslib.tables.names.colname import Colname

# Valid header names
valid_headers = [
    Colname("customername"),   # All lowercase letters
    Colname("order123"),       # Letters and numbers
    Colname("customer_name"),   # Underscored column name
    Colname("CUSTOMERNAME"),   # Automatically converted to customername
    Colname("ORDER123"),       # Automatically converted to order123
]

# Invalid header names (will raise ValueError)
try:
    Colname("customer name")   # No spaces
except ValueError as e:
    print(e)

try:
    Colname("order-date")      # No hyphens
except ValueError as e:
    print(e)

try:
    Colname("")                # Cannot be empty
except ValueError as e:
    print(e)
```

### 3. NamedList
**Purpose:** A specialised list for managing collections of string names with automatic lowercase conversion.

**Features:**
- Extends built-in `list` functionality
- Automatically converts all items to lowercase
- Provides utility methods for list operations
- JSON serialisation support
- Set operations for overlap and extension

**Examples:**
```python
from transformslib.tables.names.lists import NamedList

# Create a named list
var_list = NamedList(["name", "age", "city"])
print(var_list)  # NamedList(['name', 'age', 'city'])

# Properties and methods
print(f"Count: {var_list.count}")  # Count: 3
print(var_list.to_json())  # JSON representation

# Set operations
other_list = ["age", "salary", "department"]
overlap = var_list.overlap(other_list)
print(overlap)  # NamedList(['age'])

# Extend with unique values
var_list.extend_with(other_list)
print(var_list)  # NamedList(['name', 'age', 'city', 'salary', 'department'])
```

### 4. ColList
**Purpose:** A validated list for variable names that must conform to header naming conventions.

**Features:**
- Extends `NamedList` functionality
- Validates all items using `Colname` rules
- Ensures all variable names are properly formatted
- Raises `ValueError` for invalid formats
- Automatic lowercase conversion

**Examples:**
```python
from naming_standards import ColList

# Valid variable list
valid_vars = ColList(["customerid", "ordernumber", "amount123"])
print(valid_vars)  # ColList(['customerid', 'ordernumber', 'amount123'])

# Invalid variable list (will raise ValueError)
try:
    invalid_vars = ColList(["customer id", "order-number", "amount@"])
except ValueError as e:
    print(e)  # Column names must be in correct format

# All NamedList methods are available
print(valid_vars.to_json())
other_vars = ["productid", "customerid"]
overlap = valid_vars.overlap(other_vars)
print(overlap)  # ColList(['customerid'])
```


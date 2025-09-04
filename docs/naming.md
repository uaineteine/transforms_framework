# Naming

The naming module provides standardised classes for validating and managing table names, column headers, and variable lists. These utilities ensure consistent naming conventions across your data pipeline.

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
from transformslib.tables.names.tablename import Tablename

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

### 2. Headername
**Purpose:** Validates and standardises column header names with strict formatting rules.

**Features:**
- Enforces uppercase-only column headers
- Automatically converts input to uppercase
- Allows only letters and digits
- No spaces, symbols, or special characters permitted
- Extends the built-in `str` class for seamless integration

**Validation Rules:**
- Must not be empty
- Must contain only uppercase letters (A-Z) and digits (0-9)
- No spaces, underscores, or special characters
- Input is automatically converted to uppercase

**Examples:**
```python
from transformslib.tables.names.headername import Headername

# Valid header names
valid_headers = [
    Headername("CUSTOMERNAME"),   # All caps letters
    Headername("ORDER123"),       # Letters and numbers
    Headername("customername"),   # Automatically converted to CUSTOMERNAME
    Headername("order123"),       # Automatically converted to ORDER123
]

# Invalid header names (will raise ValueError)
try:
    Headername("CUSTOMER_NAME")   # No underscores
except ValueError as e:
    print(e)

try:
    Headername("ORDER DATE")      # No spaces
except ValueError as e:
    print(e)

try:
    Headername("")                # Cannot be empty
except ValueError as e:
    print(e)
```

### 3. NamedList
**Purpose:** A specialised list for managing collections of string names with automatic capitalisation.

**Features:**
- Extends built-in `list` functionality
- Automatically converts all items to uppercase
- Provides utility methods for list operations
- JSON serialisation support
- Set operations for overlap and extension

**Examples:**
```python
from transformslib.tables.names.lists import NamedList

# Create a named list
var_list = NamedList(["name", "age", "city"])
print(var_list)  # NamedList(['NAME', 'AGE', 'CITY'])

# Properties and methods
print(f"Count: {var_list.count}")  # Count: 3
print(var_list.to_json())  # JSON representation

# Set operations
other_list = ["AGE", "SALARY", "DEPARTMENT"]
overlap = var_list.overlap(other_list)
print(overlap)  # NamedList(['AGE'])

# Extend with unique values
var_list.extend(other_list)
print(var_list)  # NamedList(['NAME', 'AGE', 'CITY', 'SALARY', 'DEPARTMENT'])
```

### 4. VarList
**Purpose:** A validated list for variable names that must conform to header naming conventions.

**Features:**
- Extends `NamedList` functionality
- Validates all items using `Headername` rules
- Ensures all variable names are properly formatted
- Raises `ValueError` for invalid formats
- Automatic uppercase conversion

**Examples:**
```python
from transformslib.tables.names.lists import VarList

# Valid variable list
valid_vars = VarList(["customerid", "ordernumber", "amount123"])
print(valid_vars)  # VarList(['CUSTOMERID', 'ORDERNUMBER', 'AMOUNT123'])

# Invalid variable list (will raise ValueError)
try:
    invalid_vars = VarList(["customer_id", "order number", "amount"])
except ValueError as e:
    print(e)  # Column names must be in correct format

# All NamedList methods are available
print(valid_vars.to_json())
other_vars = ["PRODUCTID", "CUSTOMERID"]
overlap = valid_vars.overlap(other_vars)
print(overlap)  # VarList(['CUSTOMERID'])
```


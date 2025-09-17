# Naming Standards

A validation package for database table and column names, providing standardized classes for validating and managing table names, column headers, and variable lists.

## Features

- **Tablename**: Validates database table identifiers according to naming conventions
- **Colname**: Validates column/header names with lowercase enforcement  
- **NamedList**: A list with enforced lowercase and utility methods for name management
- **ColList**: A validated list for variable names that conform to header naming conventions

## Installation

```bash
pip install naming-standards
```

## Usage

### Table Names

```python
from transforms_names import Tablename

# Valid table names
table1 = Tablename("my_table")        # Standard format
table2 = Tablename("_private_table")  # Starting with underscore
table3 = Tablename("123")             # Purely numeric
table4 = Tablename("table123")        # Mixed alphanumeric
```

### Column Names

```python
from transforms_names import Colname

# Valid column names (automatically lowercased)
col1 = Colname("customer_name")  # Valid
col2 = Colname("order_123")      # Valid
col3 = Colname("CustomerName")   # Becomes "customername"
```

### Lists

```python
from transforms_names import ColList, NamedList

# Basic named list
names = NamedList(["CustomerID", "OrderNumber"])  # Becomes ["customerid", "ordernumber"]

# Validated column list
columns = ColList(["customerid", "ordernumber", "amount123"])
```

## Version

1.0.0
# Transforms

## Overview

The `Transform` framework encapsulates data transformations in a modular, auditable, and extensible way. Each transform is a subclass of `PipelineEvent` and logs itself when applied, ensuring full auditability of data changes.

---

## UML Diagram

![UML diagram](../diagrams/transforms.png)

---

## Transform Classes

### Purpose
Encapsulate and standardize data transformation logic.

### Key Features
- **Auditability:** Each transform logs itself when applied.
- **Extensibility:** Abstract base class `Transform` defines the interface for all transformations.

---

## Example Usage

```python
from transforms import DropVariable

tbl = DropVariable("age")(tbl)
```

---

## Example: Importing the Library

Below is a screenshot showing how to import and use the transforms library in your code:

![Example: Importing the library](https://raw.githubusercontent.com/uaineteine/transforms_framework/refs/heads/master/docs/screenshots/example_importing%20library.PNG)


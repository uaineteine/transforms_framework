# Transforms

![UML diagram](../diagrams/transforms.png)

### 1. Transform and Subclasses
**Purpose:** Encapsulates data transformations.

**Features:**
- Each transform is a subclass of `PipelineEvent` and logs itself when applied, ensuring auditability.
- `Transform` is the abstract base class for all transformations, defining the interface for transform execution.
- `TableTransform` is a base class for transformations that operate on entire tables.
- `SimpleTransform` is a base class for transformations that operate on a single variable or column within a table.
- `DropVariable` is a concrete example, removing a specified column from a DataFrame.

**Example:**
```python
from transforms import DropVariable

tbl = DropVariable("age")(tbl)
```


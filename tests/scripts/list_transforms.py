#append ../../transformslib package
import sys
sys.path.append("../..")
from transformslib.transforms.atomiclib import _discover_transforms
from transformslib.transforms.macrolib import _discover_macros

#print the total count of macros found
macros = _discover_macros()
print(f"Total macros found: {len(macros)}")

# print the total count of transforms found
transforms = _discover_transforms()
print(f"Total transforms found: {len(transforms)}")


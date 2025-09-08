# Documentation Build Process

This document outlines the process for building the documentation using Sphinx. It covers the pre-compilation steps, the use of auto-modules for API documentation, the role of meta files in providing project metadata, and the execution of the batch file to complete the build.

## Pre-compilation with Sphinx

Sphinx is a documentation generator that converts reStructuredText files into various output formats, such as HTML. The pre-compilation step involves processing the source files to prepare them for Sphinx. This includes generating necessary files and configuring Sphinx to correctly build the documentation.

### `pre_compile_source.py`

This script is responsible for pre-compiling the documentation source. It performs the following tasks:

1.  **Generating Auto-Module Documentation:** Extracts docstrings from the Python modules and generates reStructuredText files for the auto-module feature.
2.  **Populating `index.rst`:** Reads the existing `index.rst_pre` and populates the index file with module and general documentation.


To execute the pre-compilation, run:

```shell
python pre_compile_source.py
```

## Auto-Modules

Auto-modules are a Sphinx feature that automatically generates documentation from the docstrings in your Python code. This allows you to keep your API documentation up-to-date with minimal effort.

The `pre_compile_source.py` script generates `.rst` files with the automodule and autoclass directives. These directives tell Sphinx to extract the docstrings from the specified modules and classes and include them in the documentation.

## Meta Files

Meta files contain project metadata, such as the project name, version, description, and author. These files are used to populate the documentation with relevant information.

The following meta files are used:

*   `meta/description.txt`: Contains a brief description of the project.
*   `meta/purpose.txt`: Explains the purpose of the project.
*   `meta/version.txt`: Specifies the current version of the project.
*   `meta/changelog.txt`: Provides a log of changes made to the project over time.

These files are read by Sphinx during the documentation build process and their contents are inserted into the appropriate places in the documentation.

## Build Process

The `builddocs.bat` file automates the entire documentation build process. It executes the following steps:

1.  **Pre-compilation:** Runs the `pre_compile_source.py` script to generate auto-module documentation and update the index file.
2.  **Sphinx Build:** Invokes Sphinx to build the documentation from the reStructuredText files. This includes converting the `.rst` files into HTML, PDF, or other output formats.

### `builddocs.bat`

```batch
@echo off

echo Pre-compiling source files...
python pre_compile_source.py

echo Building documentation with Sphinx...
make html

echo Documentation build complete.
pause
```

To build the documentation, simply run the `builddocs.bat` file:

```shell
builddocs.bat
```

This will generate the documentation in the `docs/build/html` directory.

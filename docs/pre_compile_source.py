import os

from uainepydat import fileio
from uainepydat import datatransform

relative_directory = "../src"  # Your source code root
absolute_directory = os.path.abspath(relative_directory)

# --- Recursive listing of all .py files ---
def list_python_files(base_dir):
    python_files = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".py") and file != "__init__.py":
                python_files.append(os.path.join(root, file))
    return python_files

python_files = list_python_files(absolute_directory)

# --- Convert file paths to module import paths ---
def filepath_to_module(filepath, base_dir):
    # Remove base_dir prefix
    rel_path = os.path.relpath(filepath, base_dir)
    # Remove extension
    module_path = os.path.splitext(rel_path)[0]
    # Convert folder separators to dots
    return module_path.replace(os.path.sep, ".")

modules = [filepath_to_module(f, absolute_directory) for f in python_files]
modules = sorted(modules)

print("Python modules identified in the directory:", relative_directory)
for module in modules:
    print(module)

# --- Make auto modules ---
def rst_text(module_name):
    rst = module_name + "\n" + "=" * len(module_name) + "\n"
    return rst + "\n.. automodule:: " + module_name + "\n   :members:\n"

rst_lines = list(map(rst_text, modules))

# --- Update pre-compile index.rst_pre ---
pre_compile_path = "source/index.rst_pre"
pre_str = fileio.read_file_to_string(pre_compile_path)
post_str = datatransform.replace_between_tags(pre_str, "automodule", rst_lines, deleteTags=True)

# --- Dependencies ---
requirements_path = "../requirements.txt"
requirements = fileio.read_file_to_string(requirements_path)
requirements = datatransform.break_into_lines(requirements)
requirements = list(map(lambda string: datatransform.add_prefix(string, "* "), requirements))
requirements.append("\n")
post_str = datatransform.replace_between_tags(post_str, "dependencies", requirements, deleteTags=True)

# --- Purpose ---
purpose_path = "../meta/purpose.txt"
pur = fileio.read_file_to_string(purpose_path)
pur = datatransform.break_into_lines(pur)
post_str = datatransform.replace_between_tags(post_str, "purpose", pur, deleteTags=True)

# --- Changelog ---
changelog_path = "../meta/changelog.txt"
chlog = fileio.read_file_to_string(changelog_path)
chlog = datatransform.break_into_lines(chlog)
post_str = datatransform.replace_between_tags(post_str, "changelog", chlog, deleteTags=True)

# --- Optional description ---
description_path = "../meta/description.txt"
if os.path.exists(description_path):
    description_content = fileio.read_file_to_string(description_path)
    description_content = datatransform.break_into_lines(description_content)
    post_str = datatransform.replace_between_tags(post_str, "description", description_content, deleteTags=True)
else:
    print(f"Warning: {description_path} does not exist. Skipping description replacement.")

# --- Write out final index.rst ---
post_compile_path = "source/index.rst"
with open(post_compile_path, "w") as text_file:
    text_file.write(post_str)

print("Updated rst file")

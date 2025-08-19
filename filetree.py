import os
import sys

def print_file_tree(start_path, indent="", only_py=False):
    try:
        for item in os.listdir(start_path):
            path = os.path.join(start_path, item)
            if os.path.isdir(path):
                # Check if folder contains relevant files
                if not only_py or contains_py_files(path):
                    print(f"{indent}📁 {item}")
                    print_file_tree(path, indent + "    ", only_py)
            elif not only_py or item.endswith(".py"):
                icon = "🐍" if item.endswith(".py") else "📄"
                print(f"{indent}{icon} {item}")
    except PermissionError:
        print(f"{indent}🚫 [Permission Denied] {start_path}")

def contains_py_files(folder):
    for root, dirs, files in os.walk(folder):
        if any(f.endswith(".py") for f in files):
            return True
    return False

if __name__ == "__main__":
    # Check for argument
    only_py = len(sys.argv) > 1 and sys.argv[1] == ".py"
    start_dir = "."  # current directory
    print_file_tree(start_dir, only_py=only_py)

import shutil
import subprocess
import os

# Remove build and dist directories
for folder in ["build", "dist"]:
    if os.path.exists(folder):
        shutil.rmtree(folder)
        print(f"Removed {folder} folder.")

# Run setup_package.py to build distributions
subprocess.run(["python", "setup_package.py", "sdist", "bdist_wheel"], check=True)
print("Package built successfully.")

# Build documentation
docs_dir = "docs"
if os.path.exists(docs_dir):
    os.chdir(docs_dir)
    subprocess.run(["builddocs.bat"], check=True)
    print("Documentation built successfully.")
    os.chdir("..")

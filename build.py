import shutil
import subprocess
import os


#pre compile setup.py
# generate_setup.py

package_name = "transformslib"
version = "0.8.2"
author = ""
author_email = ""
url = ""

# Read and pre-render requirements
if os.path.exists("requirements.txt"):
    with open("requirements.txt", "r", encoding="utf-8") as f:
        requirements = f.read().splitlines()
else:
    requirements = []

# Read and pre-render README
if os.path.exists("readme.md"):
    with open("readme.md", "r", encoding="utf-8") as f:
        long_description = f.read()
else:
    long_description = "A python package of a working transforms framework."

# Generate setup.py content
setup_content = f'''from setuptools import setup, find_packages

requirements = {requirements!r}
long_description = {long_description!r}

setup(
    name="{package_name}",
    version="{version}",
    author="{author}",
    author_email="{author_email}",
    description="A python package of a working transforms framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="{url}",
    packages=find_packages(include=["{package_name}", "{package_name}.*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=requirements
)
'''

# Write the pre-rendered setup.py
with open("setup.py", "w", encoding="utf-8") as f:
    f.write(setup_content)

print("setup.py has been generated with pre-rendered requirements and README!")

# Remove build and dist directories
for folder in ["build", "dist", "transformslib.egg-info"]:
    if os.path.exists(folder):
        shutil.rmtree(folder)
        print(f"Removed {folder} folder.")

# Run setup.py to build distributions
subprocess.run(["python", "setup.py", "sdist", "bdist_wheel"], check=True)
print("Package built successfully.")

# Build documentation
docs_dir = "docs"
if os.path.exists(docs_dir):
    print("Building documentation...")
    os.chdir(docs_dir)
    try:
        # Run the same commands as in builddocs.bat but cross-platform
        subprocess.run(["python", "-m", "pre_compile_source"], check=True)
        subprocess.run(["python", "-m", "sphinx", "source", "build"], check=True)
        print("Documentation built successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Documentation build failed: {e}")
        print("Continuing without documentation build...")
    finally:
        os.chdir("..")

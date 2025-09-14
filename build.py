import shutil
import subprocess
import os
import argparse
import requests
import json
from pathlib import Path


def create_github_release(tag_name, name, body, token, repo_owner, repo_name, files_to_upload=None):
    """Create a GitHub release and upload files."""
    if not token:
        print("No GitHub token provided. Skipping release creation.")
        return False
    
    # Create the release
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/releases"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    data = {
        "tag_name": tag_name,
        "name": name,
        "body": body,
        "draft": False,
        "prerelease": False
    }
    
    print(f"Creating release {name} ({tag_name})...")
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code != 201:
        print(f"Failed to create release: {response.status_code}")
        print(response.text)
        return False
    
    release_data = response.json()
    release_id = release_data["id"]
    upload_url = release_data["upload_url"].split("{")[0]  # Remove template part
    
    print(f"Release created successfully: {release_data['html_url']}")
    
    # Upload files if provided
    if files_to_upload:
        for file_path in files_to_upload:
            if os.path.exists(file_path):
                print(f"Uploading {file_path}...")
                upload_asset(upload_url, file_path, token)
            else:
                print(f"Warning: File {file_path} not found for upload")
    
    return True

def upload_asset(upload_url, file_path, token):
    """Upload a file as a release asset."""
    filename = os.path.basename(file_path)
    
    headers = {
        "Authorization": f"token {token}",
        "Content-Type": "application/octet-stream"
    }
    
    params = {"name": filename}
    
    with open(file_path, "rb") as f:
        response = requests.post(upload_url, headers=headers, params=params, data=f)
    
    if response.status_code == 201:
        print(f"Successfully uploaded {filename}")
    else:
        print(f"Failed to upload {filename}: {response.status_code}")
        print(response.text)

def read_version_from_meta():
    """Read version from meta/version.txt file."""
    version_file = Path("meta/version.txt")
    if version_file.exists():
        return version_file.read_text().strip()
    return "0.10.0"  # fallback

def read_changelog():
    """Read changelog from meta/changelog.txt file."""
    changelog_file = Path("meta/changelog.txt")
    if changelog_file.exists():
        return changelog_file.read_text().strip()
    return "No changelog available."

def build_package():
    """Build the package distribution files."""
    package_name = "transformslib"
    version = read_version_from_meta()
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

def main():
    parser = argparse.ArgumentParser(description="Build transformslib package and optionally create GitHub release")
    parser.add_argument("--release", action="store_true", help="Create a GitHub release after building")
    parser.add_argument("--github-token", help="GitHub token for release creation (can also use GITHUB_TOKEN env var)")
    parser.add_argument("--repo-owner", default="uaineteine", help="GitHub repository owner")
    parser.add_argument("--repo-name", default="transforms_framework", help="GitHub repository name")
    
    args = parser.parse_args()
    
    # Build the package
    build_package()
    
    # Create GitHub release if requested
    if args.release:
        token = args.github_token or os.environ.get("GITHUB_TOKEN")
        if not token:
            print("Error: GitHub token required for release creation. Use --github-token or set GITHUB_TOKEN env var.")
            return 1
        
        version = read_version_from_meta()
        tag_name = f"v{version}"
        release_name = f"Release v{version}"
        changelog = read_changelog()
        
        # Find built artifacts
        dist_files = []
        if os.path.exists("dist"):
            for file in os.listdir("dist"):
                if file.endswith((".whl", ".tar.gz")):
                    dist_files.append(os.path.join("dist", file))
        
        # Create the release
        success = create_github_release(
            tag_name=tag_name,
            name=release_name,
            body=changelog,
            token=token,
            repo_owner=args.repo_owner,
            repo_name=args.repo_name,
            files_to_upload=dist_files
        )
        
        if success:
            print(f"Successfully created release {tag_name} with {len(dist_files)} artifacts")
        else:
            print("Failed to create GitHub release")
            return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
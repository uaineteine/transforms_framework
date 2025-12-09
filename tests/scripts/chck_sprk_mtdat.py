import os
import sys

def main():
    # Determine parent directory
    if len(sys.argv) < 2:
        parent_dir = os.path.join("..", "..", "templates", "jobs", "prod", "job_1")
    else:
        parent_dir = sys.argv[1]

    print(f'Searching in "{parent_dir}" ...')

    # Metadata files to check
    metadata_files = ["_SUCCESS", "_STARTING", "_COMMITTED"]

    found = False
    for root, dirs, files in os.walk(parent_dir):
        for meta in metadata_files:
            if meta in files:
                found = True
                break
        if found:
            break

    if found:
        print("ERROR: Metadata file(s) found.")
        sys.exit(1)
    else:
        print("OK: No metadata files found.")
        sys.exit(0)

if __name__ == "__main__":
    main()

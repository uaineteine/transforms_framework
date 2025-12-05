@echo off
setlocal enabledelayedexpansion

:: Parent directory to search (update this path or pass as argument)
if "%~1"=="" (
    set "PARENT_DIR=../../templates/jobs/prod/job_1"
) else (
    set "PARENT_DIR=%~1"
)

:: Flag to track if file is found
set "FOUND=0"

echo Searching in %PARENT_DIR% ...

:: Search recursively for metadata files (case-insensitive by default on Windows)
for %%F in (_SUCCESS _STARTING _COMMITTED) do (
    for /r "%PARENT_DIR%" %%G in (%%F) do (
        echo Found: %%G
        set "FOUND=1"
    )
)

:: Exit with status code
if "!FOUND!"=="1" (
    echo ERROR: Metadata file(s) found.
    exit /b 1
) else (
    echo OK: No metadata files found.
    exit /b 0
)

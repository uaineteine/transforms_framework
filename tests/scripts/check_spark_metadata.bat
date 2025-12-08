@echo off
setlocal enabledelayedexpansion

:: Parent directory to search (update this path or pass as argument)
if "%~1"=="" (
    set "PARENT_DIR=..\..\templates\jobs\prod\job_1"
) else (
    set "PARENT_DIR=%~1"
)

set "FOUND=0"

echo Searching in "%PARENT_DIR%" ...

:: Search for files named: _SUCCESS, _STARTING, _COMMITTED
for /d /r "%PARENT_DIR%" %%D in (*) do (
    for %%F in (_SUCCESS _STARTING _COMMITTED) do (
        if exist "%%D\%%F" (
            echo Found: %%D\%%F
            set "FOUND=1"
        )
    )
)

:: ==== IMPORTANT ====
:: The loop above must fully end BEFORE this IF executes
:: ====================

if "!FOUND!"=="1" (
    echo ERROR: Metadata file(s) found.
    exit /b 1
)

echo OK: No metadata files found.
exit /b 0

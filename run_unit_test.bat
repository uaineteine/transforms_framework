@echo off
echo Removing all __pycache__ folders...

REM Search and remove all __pycache__ folders recursively
for /d /r %%d in (__pycache__) do (
    if exist "%%d" (
        echo Deleting %%d
        rmdir /s /q "%%d"
    )
)

echo Done.

echo Removing the previous test_log.txt file if it exists...
if exist tests\test_log.txt del tests\test_log.txt

cd tests
cd scripts

powershell -Command "python list_transforms.py 2>&1 | Tee-Object -FilePath ../../tests/test_log.txt -Append"
if %ERRORLEVEL% NEQ 0 (
    echo list_transforms.py failed with exit code %ERRORLEVEL%
    cd ..\..
    exit /b %ERRORLEVEL%
)

cd ..
cd ..

cd templates

if exist jobs rmdir /S /Q jobs

powershell -Command "python template_pipe.py 2>&1 | Tee-Object -FilePath ../tests/test_log.txt -Append"
if %ERRORLEVEL% NEQ 0 (
    echo template_pipe.py failed with exit code %ERRORLEVEL%
    cd ..
    exit /b %ERRORLEVEL%
)

powershell -Command "python make_dag.py 2>&1 | Tee-Object -FilePath ../tests/test_log.txt -Append"
if %ERRORLEVEL% NEQ 0 (
    echo make_dag.py failed with exit code %ERRORLEVEL%
    cd ..
    exit /b %ERRORLEVEL%
)

cd ..

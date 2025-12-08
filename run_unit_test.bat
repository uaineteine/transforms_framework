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

echo. >> ../../tests/test_log.txt
echo. >> ../../tests/test_log.txt
echo ================================================================================ >> ../../tests/test_log.txt
echo Running: save_raw_text.py >> ../../tests/test_log.txt
echo ================================================================================ >> ../../tests/test_log.txt
echo. >> ../../tests/test_log.txt

powershell -Command "$output = python save_raw_text.py 2>&1; $exitCode = $LASTEXITCODE; $output | Tee-Object -FilePath ../../tests/test_log.txt -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo save_raw_text.py failed with exit code %ERRORLEVEL%
    cd ..\..
    exit /b %ERRORLEVEL%
)

echo. >> ../../tests/test_log.txt
echo. >> ../../tests/test_log.txt
echo ================================================================================ >> ../../tests/test_log.txt
echo Running: list_transforms.py >> ../../tests/test_log.txt
echo ================================================================================ >> ../../tests/test_log.txt
echo. >> ../../tests/test_log.txt

powershell -Command "$output = python list_transforms.py 2>&1; $exitCode = $LASTEXITCODE; $output | Tee-Object -FilePath ../../tests/test_log.txt -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo list_transforms.py failed with exit code %ERRORLEVEL%
    cd ..\..
    exit /b %ERRORLEVEL%
)

cd ..
cd ..

cd templates

if exist jobs rmdir /S /Q jobs

echo. >> ../tests/test_log.txt
echo. >> ../tests/test_log.txt
echo ================================================================================ >> ../tests/test_log.txt
echo Running: template_pipe.py >> ../tests/test_log.txt
echo ================================================================================ >> ../tests/test_log.txt
echo. >> ../tests/test_log.txt

powershell -Command "$output = python template_pipe.py 2>&1; $exitCode = $LASTEXITCODE; $output | Tee-Object -FilePath ../tests/test_log.txt -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo template_pipe.py failed with exit code %ERRORLEVEL%
    cd ..
    exit /b %ERRORLEVEL%
)

echo. >> ../tests/test_log.txt
echo. >> ../tests/test_log.txt
echo ================================================================================ >> ../tests/test_log.txt
echo Running: make_dag.py >> ../tests/test_log.txt
echo ================================================================================ >> ../tests/test_log.txt
echo. >> ../tests/test_log.txt

powershell -Command "$output = python make_dag.py 2>&1; $exitCode = $LASTEXITCODE; $output | Tee-Object -FilePath ../tests/test_log.txt -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo make_dag.py failed with exit code %ERRORLEVEL%
    cd ..
    exit /b %ERRORLEVEL%
)

cd ..

cd tests
cd scripts

:: Call the batch file and log its output live + to file
powershell -Command ^
    "$output = & { call check_spark_metadata.bat } 2>&1; ^
     $exitCode = $LASTEXITCODE; ^
     $output | Tee-Object -FilePath tests\test_log.txt -Append; ^
     exit $exitCode"

if %ERRORLEVEL% NEQ 0 (
    echo check_spark_metadata.bat failed with exit code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
) else (
    echo check_spark_metadata.bat completed successfully.
)

cd ..
cd ..

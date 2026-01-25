@echo off

set LOG_FILE=..\test_log.txt

echo Removing all __pycache__ folders...

REM Search and remove all __pycache__ folders recursively
for /d /r %%d in (__pycache__) do (
    if exist "%%d" (
        echo Deleting %%d
        rmdir /s /q "%%d"
    )
)

echo Done.

echo Removing the previous %LOG_FILE% file if it exists...
if exist %LOG_FILE% del %LOG_FILE%

echo Starting unit tests...

cd tests
cd scripts

:: Check if --express flag is passed
if "%1"=="--express" goto EXPRESS_MODE

echo. >> %LOG_FILE%
echo. >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo Running: save_raw_text.py >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo. >> %LOG_FILE%

powershell -Command "$output = python save_raw_text.py 2>&1; $exitCode = $LASTEXITCODE; $output = $output -replace '[^\u0000-\u007F]', ''; $output | Out-String -Stream | Out-File -FilePath %LOG_FILE% -Encoding utf8 -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo save_raw_text.py failed with exit code %ERRORLEVEL%
    cd ..\..
    exit /b %ERRORLEVEL%
)

echo. >> %LOG_FILE%
echo. >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo Running: list_transforms.py >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo. >> %LOG_FILE%

powershell -Command "$output = python list_transforms.py 2>&1; $exitCode = $LASTEXITCODE; $output = $output -replace '[^\u0000-\u007F]', ''; $output | Out-String -Stream | Out-File -FilePath %LOG_FILE% -Encoding utf8 -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo list_transforms.py failed with exit code %ERRORLEVEL%
    cd ..\..
    exit /b %ERRORLEVEL%
)

echo. >> %LOG_FILE%
echo. >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo Running: load_resources.py >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo. >> %LOG_FILE%

powershell -Command "$output = python load_resources.py 2>&1; $exitCode = $LASTEXITCODE; $output = $output -replace '[^\u0000-\u007F]', ''; $output | Out-String -Stream | Out-File -FilePath %LOG_FILE% -Encoding utf8 -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo load_resources.py failed with exit code %ERRORLEVEL%
    cd ..\..
    exit /b %ERRORLEVEL%
)

:EXPRESS_MODE

if exist jobs rmdir /S /Q jobs

echo. >> %LOG_FILE%
echo. >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo Running: execute_transforms.py >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo. >> %LOG_FILE%

powershell -Command "$output = python execute_transforms.py 2>&1; $exitCode = $LASTEXITCODE; $output = $output -replace '[^\u0000-\u007F]', ''; $output | Out-String -Stream | Out-File -FilePath %LOG_FILE% -Encoding utf8 -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo execute_transforms.py failed with exit code %ERRORLEVEL%
    cd ..
    exit /b %ERRORLEVEL%
)

echo. >> %LOG_FILE%
echo. >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo Running: make_dag.py >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo. >> %LOG_FILE%

powershell -Command "$output = python make_dag.py 2>&1; $exitCode = $LASTEXITCODE; $output = $output -replace '[^\u0000-\u007F]', ''; $output | Out-String -Stream | Out-File -FilePath %LOG_FILE% -Encoding utf8 -Append; exit $exitCode"
if %ERRORLEVEL% NEQ 0 (
    echo make_dag.py failed with exit code %ERRORLEVEL%
    cd ..
    exit /b %ERRORLEVEL%
)

echo. >> %LOG_FILE%
echo. >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo Running: chck_sprk_mtdat.py >> %LOG_FILE%
echo ================================================================================ >> %LOG_FILE%
echo. >> %LOG_FILE%

:: Call the batch file and log its output live + to file
powershell -Command "$output = & { python .\chck_sprk_mtdat.py } 2>&1; $exitCode = $LASTEXITCODE; $output = $output -replace '[^\u0000-\u007F]', ''; $output | Out-String -Stream | Out-File -FilePath %LOG_FILE% -Encoding utf8 -Append; exit $exitCode"

if %ERRORLEVEL% NEQ 0 (
    echo chck_sprk_mtdat.py failed with exit code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
) else (
    echo chck_sprk_mtdat.py completed successfully.
)

cd ..
cd ..

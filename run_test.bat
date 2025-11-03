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


cd templates

rmdir /S /Q jobs

powershell -Command "python template_pipe.py | Tee-Object -FilePath test_log.txt"

python make_dag.py

cd ..
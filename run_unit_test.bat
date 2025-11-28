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

powershell -Command "python save_raw_text.py | Tee-Object -FilePath ../../tests/test_log.txt -Append"

del "test_tables/unit_test_subpackage/this_is_a_test.txt"

powershell -Command "python list_transforms.py | Tee-Object -FilePath ../../tests/test_log.txt -Append"

cd ..
cd ..

cd templates

rmdir /S /Q jobs

powershell -Command "python template_pipe.py | Tee-Object -FilePath ../tests/test_log.txt -Append"

powershell -Command "python make_dag.py | Tee-Object -FilePath ../tests/test_log.txt -Append"

cd ..

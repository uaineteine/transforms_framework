@echo off

rmdir /S /Q build
rmdir /S /Q dist

python setup_package.py sdist bdist_wheel

REM Build documentation
call cd docs
call builddocs.bat
echo Documentation built successfully.
call cd ..

@echo off

rmdir /S /Q build
rmdir /S /Q dist

python setup_package.py sdist bdist_wheel

REM Build documentation
@REM call cd docs
@REM call builddocs.bat
@REM echo Documentation built successfully.
@REM call cd ..

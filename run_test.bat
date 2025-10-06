cd templates

rmdir /S /Q jobs

powershell -Command "python template_pipe.py | Tee-Object -FilePath test_log.txt"

python make_dag.py

cd ..
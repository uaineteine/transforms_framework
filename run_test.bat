cd templates

rmdir /S /Q events_log

powershell -Command "python template_pipe.py | Tee-Object -FilePath test_log.txt"

python make_dag.py

cd ..
cd templates

powershell -Command "python template_load_pipe.py | Tee-Object -FilePath test_log.txt"

cd ..
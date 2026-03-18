import sys
import os

def pytest_collect_file(parent, file_path):
    if file_path.name == "test_cases.py":
        test_dir = str(file_path.parent)
        if test_dir not in sys.path:
            sys.path.insert(0, test_dir)
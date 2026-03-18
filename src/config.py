import os
import sys

PYTHON_PATH = r"C:\Users\Swati\PycharmProjects\pyspark-assignment\.venv_new\Scripts\python.exe"

os.environ["PYSPARK_PYTHON"]        = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\Users\Swati\PycharmProjects\PySpark\hadoop"

os.environ["PATH"] = (
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot\bin;"
    r"C:\Users\Swati\PycharmProjects\PySpark\hadoop\bin;"
    + os.environ["PATH"]
)
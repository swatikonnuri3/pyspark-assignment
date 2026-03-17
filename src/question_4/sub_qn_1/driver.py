import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from util import get_spark, read_json_dynamic


def main():
    spark = get_spark()

    # Update this path to where your JSON file is located
    json_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'employee.json')

    print("=== Raw JSON DataFrame ===")
    df = read_json_dynamic(spark, json_path)
    df.show(truncate=False)
    df.printSchema()


if __name__ == "__main__":
    main()
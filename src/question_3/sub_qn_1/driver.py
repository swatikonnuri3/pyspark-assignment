import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from util import get_spark, create_log_df


def main():
    spark = get_spark()
    df = create_log_df(spark)

    print("=== User Activity Log DataFrame ===")
    df.show(truncate=False)
    df.printSchema()


if __name__ == "__main__":
    main()
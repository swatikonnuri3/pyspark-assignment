import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from util import (get_spark, create_credit_card_df_inline,
                  create_credit_card_df_csv, create_credit_card_df_json,
                  create_credit_card_df_parquet)


def main():
    spark = get_spark()

    print("=== Method 1: createDataFrame inline ===")
    create_credit_card_df_inline(spark).show(truncate=False)

    print("=== Method 2: Read from CSV ===")
    create_credit_card_df_csv(spark).show(truncate=False)

    print("=== Method 3: Read from JSON ===")
    create_credit_card_df_json(spark).show(truncate=False)

    print("=== Method 4: Read from Parquet ===")
    create_credit_card_df_parquet(spark).show(truncate=False)


if __name__ == "__main__":
    main()
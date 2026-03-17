from pyspark.sql.functions import countDistinct, col


def new_products(prod_df, pur_df):
    """
    Customers who bought ALL models listed in product_data_df.
    Logic:
      - total = distinct count of products in catalogue
      - group purchases by customer, count their distinct models
      - keep customers whose count == total
    """
    total = prod_df.select("product_model").distinct().count()

    return (
        pur_df.groupBy("customer")
              .agg(countDistinct(col("product_model")).alias("count"))
              .filter(col("count") == total)
              .select("customer")
    )
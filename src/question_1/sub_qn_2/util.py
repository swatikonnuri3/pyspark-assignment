from pyspark.sql.functions import col


def customers_only_iphone13(purchase_data_df):
    """
    Customers who bought ONLY iphone13 and nothing else.
    Logic: has iphone13  MINUS  has any other product
    """
    has_iphone13 = purchase_data_df.filter(
        col("product_model") == "iphone13"
    ).select("customer").distinct()

    has_other = purchase_data_df.filter(
        col("product_model") != "iphone13"
    ).select("customer").distinct()

    return has_iphone13.subtract(has_other)
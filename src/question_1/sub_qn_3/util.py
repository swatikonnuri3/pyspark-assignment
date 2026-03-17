from pyspark.sql.functions import col


def customers_upgraded_to_iphone14(purchase_data_df):
    """
    Customers who bought iphone13 AND later bought iphone14 (upgrade).
    Logic: has iphone13  INTERSECT  has iphone14
    """
    has_iphone13 = purchase_data_df.filter(
        col("product_model") == "iphone13"
    ).select("customer").distinct()

    has_iphone14 = purchase_data_df.filter(
        col("product_model") == "iphone14"
    ).select("customer").distinct()

    return has_iphone13.intersect(has_iphone14)
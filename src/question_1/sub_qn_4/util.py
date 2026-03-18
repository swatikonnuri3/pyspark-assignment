import sys
import os
import importlib.util as importlib_util
from pyspark.sql.functions import countDistinct, col

# Load sub_qn_1's util explicitly by file path
_sub1_path = os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1', 'util.py')
_spec = importlib_util.spec_from_file_location("sub_qn_1_util", _sub1_path)
_sub_qn_1_util = importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_sub_qn_1_util)

# re-export
create_purchase_data_df = _sub_qn_1_util.create_purchase_data_df
create_product_data_df = _sub_qn_1_util.create_product_data_df


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
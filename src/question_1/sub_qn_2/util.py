import sys
import os
import importlib.util as importlib_util
from pyspark.sql.functions import col

# Load sub_qn_1's util explicitly by file path (avoids circular import)
_sub1_path = os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1', 'util.py')
_spec = importlib_util.spec_from_file_location("sub_qn_1_util", _sub1_path)
_sub_qn_1_util = importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_sub_qn_1_util)

# re-export
create_purchase_data_df = _sub_qn_1_util.create_purchase_data_df
create_product_data_df = _sub_qn_1_util.create_product_data_df


def customers_only_iphone13(purchase_data_df):
    has_iphone13 = purchase_data_df.filter(
        col("product_model") == "iphone13"
    ).select("customer").distinct()

    has_other = purchase_data_df.filter(
        col("product_model") != "iphone13"
    ).select("customer").distinct()

    return has_iphone13.subtract(has_other)
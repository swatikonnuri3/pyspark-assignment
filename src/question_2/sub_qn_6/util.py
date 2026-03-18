import sys
import os
import importlib.util as importlib_util

sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

# Load sub_qn_1's util explicitly by file path
_sub1_path = os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1', 'util.py')
_spec = importlib_util.spec_from_file_location("sub_qn_1_util", _sub1_path)
_sub_qn_1_util = importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_sub_qn_1_util)

# Load sub_qn_5's util explicitly by file path
_sub5_path = os.path.join(os.path.dirname(__file__), '..', 'sub_qn_5', 'util.py')
_spec5 = importlib_util.spec_from_file_location("sub_qn_5_util", _sub5_path)
_sub_qn_5_util = importlib_util.module_from_spec(_spec5)
_spec5.loader.exec_module(_sub_qn_5_util)

# re-export
create_credit_card_df = _sub_qn_1_util.create_credit_card_df
apply_mask = _sub_qn_5_util.apply_mask


def get_final_output(spark):
    """Returns DataFrame with card_number and masked_card_number columns."""
    df = create_credit_card_df(spark)
    return apply_mask(df).select("card_number", "masked_card_number")
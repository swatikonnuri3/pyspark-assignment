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

# re-export
create_credit_card_df = _sub_qn_1_util.create_credit_card_df


def decrease_partitions(df, original):
    """Decrease partitions using coalesce (no full shuffle)."""
    return df.coalesce(original)
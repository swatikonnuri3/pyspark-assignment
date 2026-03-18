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
create_log_df = _sub_qn_1_util.create_log_df


def rename_columns(df):
    mapping = {
        "log id":    "log_id",
        "user$id":   "user_id",
        "action":    "user_activity",
        "timestamp": "time_stamp",
    }
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df
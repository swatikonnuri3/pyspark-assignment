import sys
import os
import importlib.util as importlib_util

sys.path.insert(0, r"C:\Users\Swati\PycharmProjects\pyspark-assignment\src")
import config

# Load sub_qn_1's util
_sub1_path = os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1', 'util.py')
_spec = importlib_util.spec_from_file_location("sub_qn_1_util", _sub1_path)
_sub_qn_1_util = importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_sub_qn_1_util)

# Load sub_qn_2's util
_sub2_path = os.path.join(os.path.dirname(__file__), '..', 'sub_qn_2', 'util.py')
_spec2 = importlib_util.spec_from_file_location("sub_qn_2_util", _sub2_path)
_sub_qn_2_util = importlib_util.module_from_spec(_spec2)
_spec2.loader.exec_module(_sub_qn_2_util)

# re-export
create_log_df = _sub_qn_1_util.create_log_df
rename_columns = _sub_qn_2_util.rename_columns

from pyspark.sql.functions import col, to_date, to_timestamp, date_format

def convert_timestamp_to_date(df):
    """
    Convert time_stamp column to login_date with YYYY-MM-DD format
    and DateType as its data type.
    """
    return (
        df.withColumn(
            "login_date",
            to_date(to_timestamp(col("time_stamp")), "yyyy-MM-dd")
        )
        .drop("time_stamp")
    )
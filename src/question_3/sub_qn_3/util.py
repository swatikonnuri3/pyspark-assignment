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

from pyspark.sql.functions import col, count, to_timestamp, lit
from datetime import datetime, timedelta

def actions_last_7_days(df):
    """
    Count number of actions performed by each user in last 7 days.
    Uses max date in dataset as reference point.
    """
    df = df.withColumn("time_stamp", to_timestamp(col("time_stamp")))
    max_date = df.agg({"time_stamp": "max"}).collect()[0][0]
    seven_days_ago = max_date - timedelta(days=7)
    return (
        df.filter(col("time_stamp") >= lit(seven_days_ago))
          .groupBy("user_id")
          .agg(count("user_activity").alias("action_count"))
          .orderBy("user_id")
    )
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
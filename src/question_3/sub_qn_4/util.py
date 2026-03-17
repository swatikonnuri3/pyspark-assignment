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
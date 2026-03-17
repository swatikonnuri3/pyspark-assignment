def rename_columns(df):
    """
    Dynamically rename columns to snake_case.
    log id   -> log_id
    user$id  -> user_id
    action   -> user_activity
    timestamp-> time_stamp
    """
    mapping = {
        "log id":    "log_id",
        "user$id":   "user_id",
        "action":    "user_activity",
        "timestamp": "time_stamp",
    }
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df
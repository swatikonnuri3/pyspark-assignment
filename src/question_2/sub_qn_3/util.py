def increase_partitions(df, num=5):
    """Increase partitions using repartition (full shuffle)."""
    return df.repartition(num)
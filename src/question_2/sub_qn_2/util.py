def get_partition_count(df):
    """Print and return number of partitions."""
    count = df.rdd.getNumPartitions()
    print(f"Number of partitions: {count}")
    return count
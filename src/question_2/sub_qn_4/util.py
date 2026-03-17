def decrease_partitions(df, original):
    """Decrease partitions using coalesce (no full shuffle)."""
    return df.coalesce(original)
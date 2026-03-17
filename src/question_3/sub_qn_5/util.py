def write_csv_options(df, base_path="output/q3"):
    """
    Write DataFrame as CSV with different write options.
    Covers: overwrite, append, header, delimiter, quote, nullValue, dateFormat
    """

    # Option 1 - overwrite with header
    df.write.mode("overwrite") \
        .option("header", True) \
        .csv(f"{base_path}/csv_overwrite")

    # Option 2 - append mode
    df.write.mode("append") \
        .option("header", True) \
        .csv(f"{base_path}/csv_append")

    # Option 3 - custom delimiter
    df.write.mode("overwrite") \
        .option("header", True) \
        .option("delimiter", "|") \
        .csv(f"{base_path}/csv_pipe")

    # Option 4 - with nullValue and dateFormat
    df.write.mode("overwrite") \
        .option("header",     True) \
        .option("nullValue",  "N/A") \
        .option("dateFormat", "yyyy-MM-dd") \
        .csv(f"{base_path}/csv_nulldate")

    print(f"All CSV files written to {base_path}")
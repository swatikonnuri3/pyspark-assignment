def write_managed_table(df, spark):
    """
    Write DataFrame as a managed table.
    Database: user
    Table:    login_details
    Mode:     overwrite
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS user")

    df.write.mode("overwrite") \
        .saveAsTable("user.login_details")

    print("Table user.login_details created successfully.")
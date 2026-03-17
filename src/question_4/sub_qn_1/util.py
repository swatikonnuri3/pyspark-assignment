from pyspark.sql import SparkSession


def get_spark():
    return SparkSession.builder.appName("Q4").master("local").getOrCreate()


def read_json_dynamic(spark, path):
    """
    Read JSON file dynamically using multiLine option.
    Schema is inferred automatically.
    """
    return spark.read \
        .option("multiLine", True) \
        .option("inferSchema", True) \
        .json(path)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


SCHEMA = StructType([
    StructField("card_number", StringType(), True)
])

DATA = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]


def get_spark():
    return SparkSession.builder.appName("Q2").master("local").getOrCreate()

def create_credit_card_df_inline(spark):
    return spark.createDataFrame(DATA, schema=SCHEMA)

def create_credit_card_df_csv(spark, path="credit_cards.csv"):
    spark.createDataFrame(DATA, schema=SCHEMA) \
         .write.mode("overwrite").option("header", True).csv(path)
    return spark.read.option("header", True).schema(SCHEMA).csv(path)

def create_credit_card_df_json(spark, path="credit_cards.json"):
    spark.createDataFrame(DATA, schema=SCHEMA) \
         .write.mode("overwrite").json(path)
    return spark.read.schema(SCHEMA).json(path)

def create_credit_card_df_parquet(spark, path="credit_cards.parquet"):
    spark.createDataFrame(DATA, schema=SCHEMA) \
         .write.mode("overwrite").parquet(path)
    return spark.read.schema(SCHEMA).parquet(path)

def create_credit_card_df(spark):
    return create_credit_card_df_inline(spark)
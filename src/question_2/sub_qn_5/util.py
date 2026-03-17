from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def mask_card_udf():
    """UDF that masks all digits except last 4 with *."""
    def mask(card_number):
        if card_number is None:
            return None
        return "*" * (len(card_number) - 4) + card_number[-4:]
    return udf(mask, StringType())


def apply_mask(df):
    """Adds masked_card_number column to df."""
    return df.withColumn("masked_card_number", mask_card_udf()(col("card_number")))
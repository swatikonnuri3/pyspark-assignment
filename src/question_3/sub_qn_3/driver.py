import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_2'))
sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession
import util as sub1
import util as sub2
from util import actions_last_7_days

import importlib
sub1 = importlib.import_module('util')


def main():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
    import util as s1
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_2'))
    import util as s2
    sys.path.insert(0, os.path.dirname(__file__))
    from util import actions_last_7_days

    spark = SparkSession.builder.appName("Q3_3").master("local").getOrCreate()
    df = s2.rename_columns(s1.create_log_df(spark))

    print("=== Actions per user in last 7 days ===")
    actions_last_7_days(df).show(truncate=False)


if __name__ == "__main__":
    main()
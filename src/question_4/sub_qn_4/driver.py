import sys
import os
sys.path.insert(0, os.path.dirname(__file__))


def load(path, name):
    import importlib.util as ilu
    spec = ilu.spec_from_file_location(name, path)
    mod  = ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

base = os.path.join(os.path.dirname(__file__), '..')
s1   = load(os.path.join(base, 'sub_qn_1', 'util.py'), 's1')

from util import demonstrate_explode, demonstrate_explode_outer, demonstrate_posexplode
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Q4_4").master("local").getOrCreate()

    json_path = os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'data', 'employee.json'
    )
    df = s1.read_json_dynamic(spark, json_path)

    # Find the first array column dynamically
    from pyspark.sql.types import ArrayType
    array_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, ArrayType)
    ]

    if array_cols:
        array_col = array_cols[0]
        print(f"Using array column: {array_col}")
        demonstrate_explode(df, array_col)
        demonstrate_explode_outer(df, array_col)
        demonstrate_posexplode(df, array_col)


if __name__ == "__main__":
    main()
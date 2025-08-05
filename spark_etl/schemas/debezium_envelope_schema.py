from pyspark.sql.types import StructField, StructType, LongType, BooleanType, StringType

schema = StructType([
        StructField("after", StructType([
            StructField("id", LongType()),
            StructField("krs_number", StringType()),
            StructField("is_current", BooleanType()),
            StructField("raw_data", StringType())
        ]))
])
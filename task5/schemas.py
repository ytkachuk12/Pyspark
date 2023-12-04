"""Py Spark Schemas (StructType) for data files"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Contains the following information for task.1.scv:
TASK5_1_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("m1", IntegerType(), False),
        StructField("m2", IntegerType(), False),
        StructField("m3", IntegerType(), False),
        StructField("m4", IntegerType(), False),
        StructField("m5", IntegerType(), False),
        StructField("m6", IntegerType(), False),
        StructField("m7", IntegerType(), False),
        StructField("m8", IntegerType(), False),
        StructField("m9", IntegerType(), False),
        StructField("m10", IntegerType(), False),
        StructField("m11", IntegerType(), False),
        StructField("m12", IntegerType(), False),
    ]
)

# Contains the following information for task.2.scv:
TASK5_2_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("month", StringType(), False),
    ]
)

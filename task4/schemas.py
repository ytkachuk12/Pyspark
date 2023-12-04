"""Py Spark Schemas (StructType) for data files"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Contains the following information for task4_data.csv:
TASK4_DATA_SCHEMA = StructType(
    [
        StructField("patient_id", IntegerType(), False),
        StructField("fac_prof", StringType(), False),
        StructField("proc_code", StringType(), False),
    ]
)

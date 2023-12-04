"""Py Spark Schemas (StructType) for data files"""

from pyspark.sql.types import StructType, StructField, StringType


ENROLL_SCHEMA = StructType(
    [
        StructField("effective_from_date", StringType(), False),
        StructField("patient_id", StringType(), False),
    ]
)

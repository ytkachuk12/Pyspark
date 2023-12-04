"""Contains test data and test schemas"""

import datetime

from pyspark.sql.types import StructType, StructField, Row, StringType, TimestampType, IntegerType


HOSPITAL_ENCOUNTER_DATA = [
    Row(
        record_identifier='001', facility_id='110010030', fac_medicare_id=None,
        discharge_date=datetime.datetime(1998, 12, 2, 0, 0), patient_id='1'
    ),
    Row(
        record_identifier='002', facility_id='101013510', fac_medicare_id=None,
        discharge_date=datetime.datetime(2000, 12, 29, 0, 0), patient_id='158'
    ),
    Row(
        record_identifier='003', facility_id='101010330', fac_medicare_id=None,
        discharge_date=datetime.datetime(2002, 3, 14, 0, 0), patient_id='159'
    ),
    Row(
        record_identifier='004', facility_id='101082590', fac_medicare_id=None,
        discharge_date=datetime.datetime(1994, 12, 4, 0, 0), patient_id='160'
    ),
    Row(
        record_identifier='005', facility_id=None, fac_medicare_id='101082620',
        discharge_date=datetime.datetime(2000, 10, 13, 0, 0), patient_id='161'
    )
]


HOSPITAL_ENCOUNTER_EMPTY_DATA = []


RATIO9_DATA = [
    Row(
        index='1', ratio_key='1998F101061430', start_date=datetime.datetime(1998, 10, 1, 0, 0),
        end_date=datetime.datetime(1998, 12, 31, 0, 0), valid_flag=5,
        ratios='[0.700891, 0.700891, 0.933435, 0.933435, 0.231026, 0.215936, 0.215936, 0.146963]',
        f_m_flag='F', f_m_id='110010030'
    ),
    Row(
        index='2', ratio_key='1998F101061700', start_date=datetime.datetime(1998, 10, 1, 0, 0),
        end_date=datetime.datetime(1998, 12, 31, 0, 0), valid_flag=5,
        ratios='[0.578016, 0.578016, 0.75806, 0.75806, 0.170181, 0.154965, 0.154965, 0.327482]',
        f_m_flag='F', f_m_id='101013510'
    ),
    Row(
        index='3', ratio_key='1998F101062140', start_date=datetime.datetime(1998, 10, 1, 0, 0),
        end_date=datetime.datetime(1998, 12, 31, 0, 0), valid_flag=5,
        ratios='[0.898432, 0.787276, 0.765985, 0.765985, 0.280726, 0.193915, 0.193915, 0.311587]',
        f_m_flag='F', f_m_id='101082590'
    ),
    Row(
        index='4', ratio_key='1998F101063140', start_date=datetime.datetime(1998, 10, 1, 0, 0),
        end_date=datetime.datetime(1998, 12, 31, 0, 0), valid_flag=5,
        ratios='[0.998594, 2.251893, 0.431603, 0.431603, 0.167846, 0.110234, 0.110234, 0.074652]',
        f_m_flag='F', f_m_id='101063140'
    ),
    Row(
        index='5', ratio_key='1998M101063440', start_date=datetime.datetime(1998, 10, 1, 0, 0),
        end_date=datetime.datetime(1998, 11, 30, 0, 0), valid_flag=5,
        ratios='[0.760429, 0.477766, 0.532505, 0.532505, 0.30653, 0.244962, 0.244962, 0.172331]',
        f_m_flag='M', f_m_id='101082620'
    )
]


EXPECTED_DATA = [
    Row(
        record_identifier='001', facility_id='110010030', fac_medicare_id=None,
        discharge_date=datetime.datetime(1998, 12, 2, 0, 0), patient_id='1', index='1',
        ratio_key='1998F101061430', start_date=datetime.datetime(1998, 10, 1, 0, 0),
        end_date=datetime.datetime(1998, 12, 31, 0, 0), valid_flag=5,
        ratios='[0.700891, 0.700891, 0.933435, 0.933435, 0.231026, 0.215936, 0.215936, 0.146963]',
    ),
    Row(
        record_identifier='002', facility_id='101013510', fac_medicare_id=None,
        discharge_date=datetime.datetime(2000, 12, 29, 0, 0), patient_id='158', index='2',
        ratio_key='1998F101061700', start_date=datetime.datetime(1998, 10, 1, 0, 0),
        end_date=datetime.datetime(1998, 12, 31, 0, 0), valid_flag=5,
        ratios='[0.578016, 0.578016, 0.75806, 0.75806, 0.170181, 0.154965, 0.154965, 0.327482]',
    ),
    Row(
        record_identifier='005', facility_id=None, fac_medicare_id='101082620',
        discharge_date=datetime.datetime(2000, 10, 13, 0, 0), patient_id='161', index='5',
        ratio_key='1998M101063440', start_date=datetime.datetime(1998, 10, 1, 0, 0),
        end_date=datetime.datetime(1998, 11, 30, 0, 0), valid_flag=5,
        ratios='[0.760429, 0.477766, 0.532505, 0.532505, 0.30653, 0.244962, 0.244962, 0.172331]',
    ),
]


EXPECTED_EMPTY_DATA = []


HOSPITL_ENCOUNTER_SCHEMA = StructType(
    [
        StructField("record_identifier", StringType(), True),
        StructField("facility_id", StringType(), True),
        StructField("fac_medicare_id", StringType(), True),
        StructField("discharge_date", TimestampType(), True),
        StructField("patient_id", StringType(), True),
    ]
)


EXPECTED_HOSPITL_ENCOUNTER_SCHEMA = StructType(
    [
        StructField("record_identifier", StringType(), True),
        StructField("facility_id", StringType(), True),
        StructField("fac_medicare_id", StringType(), True),
        StructField("discharge_date", TimestampType(), True),
        StructField("patient_id", StringType(), True),
    ]
)


RATIO9_SCHEMA = StructType(
    [
        StructField("index", StringType(), True),
        StructField("ratio_key", StringType(), True),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("valid_flag", IntegerType(), True),
        StructField("ratios", StringType(), False),
        StructField("f_m_flag", StringType(), True),
        StructField("f_m_id", StringType(), True),
    ]
)


EXPECTED_SCHEMA = StructType(
    [
        StructField("record_identifier", StringType(), True),
        StructField("facility_id", StringType(), True),
        StructField("fac_medicare_id", StringType(), True),
        StructField("discharge_date", TimestampType(), True),
        StructField("patient_id", StringType(), True),
        StructField("index", StringType(), True),
        StructField("ratio_key", StringType(), True),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("valid_flag", IntegerType(), True),
        StructField("ratios", StringType(), False),
    ]
)

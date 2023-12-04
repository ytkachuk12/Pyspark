"""Test for task3.py"""

import os
import pytest

from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

import task3.task_3 as Func
from task3.tests.data.data import (
    HOSPITAL_ENCOUNTER_DATA, HOSPITAL_ENCOUNTER_EMPTY_DATA,
    HOSPITL_ENCOUNTER_SCHEMA, EXPECTED_HOSPITL_ENCOUNTER_SCHEMA,
    RATIO9_DATA, RATIO9_SCHEMA,
    EXPECTED_DATA, EXPECTED_EMPTY_DATA, EXPECTED_SCHEMA
)


@pytest.fixture
def spark_session():
    """Returns the SparkSession instance."""

    spark_session = (
        SparkSession.builder
        .config('spark.sql.codegen.wholeStage', 'false')
        .appName("test_app")
        .getOrCreate()
    )

    yield spark_session


@pytest.fixture
def data_frames(spark_session):
    """Returns the DataFrames instance."""

    yield Func.DataFrames()


def test_get_hospital_encounter(data_frames, spark_session):
    """Test get_hospital_encounter method."""

    # create SparkSession instance in the task_3.py file
    data_frames.session = "test"
    # Set self.df_hospital_encounter
    df_actual = data_frames.get_hospital_encounter()

    df_expected = (
        spark_session.read
        .option('header', 'true')
        .schema(EXPECTED_HOSPITL_ENCOUNTER_SCHEMA)
        .csv(os.path.join(os.path.dirname(__file__), 'data/expected_h_e.csv'))
    )

    assert_pyspark_df_equal(df_actual, df_expected)


@pytest.mark.parametrize(
    "h_e,r9,expected",
    [
        (HOSPITAL_ENCOUNTER_DATA, RATIO9_DATA, EXPECTED_DATA),
        (HOSPITAL_ENCOUNTER_EMPTY_DATA, RATIO9_DATA, EXPECTED_EMPTY_DATA),
    ]
)
def test_join_Hospital_encounter_Ratio9(data_frames, spark_session, h_e, r9, expected):
    """Test join_Hospital_encounter_Ratio9 method"""

    data_frames.df_hospital_encounter = spark_session.createDataFrame(
        h_e, HOSPITL_ENCOUNTER_SCHEMA
    )
    data_frames.df_ratio9 = spark_session.createDataFrame(
        r9, RATIO9_SCHEMA
    )

    df_actual = data_frames.join_Hospital_encounter_Ratio9(
        data_frames.df_hospital_encounter, data_frames.df_ratio9
    ).sort("record_identifier")
    df_expected = spark_session.createDataFrame(
        expected, EXPECTED_SCHEMA
    ).sort("record_identifier")

    assert_pyspark_df_equal(df_actual, df_expected)

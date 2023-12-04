"""Contains all methods for getting data frames set in the task 3"""

import os

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import add_months, array, col, unix_timestamp, substring, count

from task3.schemas import (
    HOSPITL_ENCOUNTER_SCHEMA, RATIO9_SCHEMA, HE_DROPPED_COLS, RATIO9_DROPPED_COLS
)
from task3.file_names import HOSPITL_ENCOUNTER, RATIO9
from spark_session import Spark


class DataFrames(Spark):
    """Contains all methods for getting data frames set in the tasks"""

    dirname = os.path.dirname(__file__)  # path to the folder that contains "task_3.py" file

    def __init__(self):
        self.df_hospital_encounter = None
        self.df_ratio9 = None
        super().__init__()

    def start(self) -> None:
        """Class pipeline. All class methods calling here"""

        self.df_hospital_encounter = self.get_hospital_encounter()
        self.df_ratio9 = self.get_ratio9()

        self.ratios_array()
        self.split_ratio_key()

        self.result = self.join_Hospital_encounter_Ratio9(
            self.df_hospital_encounter, self.df_ratio9
        )

    def get_hospital_encounter(self) -> DataFrame:
        """
        Read hospital_encounter csv file(file_name) with schema to data frame.
        Convert 'discharge_date' from string to TimeStamp
        Drop all unused and unnecessary columns
        Returns PySpark DataFrame
        """

        data_frame = (
            self.spark_session.read  # The DataFrameReader
            .option('sep', ',')
            .option('header', 'true')  # Use first line of all files as header
            .schema(HOSPITL_ENCOUNTER_SCHEMA)  # Use the specified schema
            # Creates a DataFrame from CSV after reading in the file
            .csv(os.path.join(self.dirname, HOSPITL_ENCOUNTER))
            # convert "discharge_date" to timestamp
            .withColumn(
                "discharge_date", unix_timestamp(col("discharge_date"), "MMddyyyy")
                .cast("timestamp")
            )
            # dropped all unused and unnecessary columns
            .drop(*HE_DROPPED_COLS)
        )

        return data_frame

    def get_ratio9(self) -> DataFrame:
        """Read ratio9 csv file(file_name) with schema to data frame."""

        data_frame = (
            self.spark_session.read  # The DataFrameReader
            .option('sep', ';')
            .option('header', 'false')  # Use first line of all files as header
            .option("comment", "#")
            .option("timestampFormat", "MM/dd/yyyy")
            .schema(RATIO9_SCHEMA)  # Use the specified schema
            # Creates a DataFrame from CSV after reading in the file
            .csv(os.path.join(self.dirname, RATIO9))
        )

        return data_frame

    def ratios_array(self) -> None:
        """
        Creates array "ratios"(ArrayType) from ratio1-ratio37 and dratio1-dratio37
        Remove ratio1-ratio37 and dratio1-dratio37 columns
        """

        self.df_ratio9 = (
            self.df_ratio9
            # Creates array "ratios"(ArrayType) from ratio1-ratio37 and dratio1-dratio37
            # and convert it to stringType
            .withColumn("ratios", array(*RATIO9_DROPPED_COLS).cast("string"))
            # remove ratio1-ratio37 and dratio1-dratio37 columns
            .drop(*RATIO9_DROPPED_COLS)
        )

    def split_ratio_key(self):
        """Creates 2 new columns from 'ratio_key': 'f_m_flag' and 'f_m_id'"""

        self.df_ratio9 = (
            self.df_ratio9
            .withColumn("f_m_flag", substring("ratio_key", 5, 1))
            .withColumn("f_m_id", substring("ratio_key", 6, 9))
        )

    def join_Hospital_encounter_Ratio9(self, df1: DataFrame, df2: DataFrame) -> DataFrame:
        """
        Join 2 dataframes(df_hospital_encounter and df_ratio9) according to the following rules:
                (HE) FacilityId = (Ratio9) Facility Id
                and
                (HE) discharge date between (Ratio9) start and end date
                if no matches: (HE) discharge date - 1 year between (Ratio9) start and end date
                if no matches: (HE) discharge date - 2 year between (Ratio9) start and end date
                if no matches: (HE) discharge date - 3 year between (Ratio9) start and end date
            if it still doesn't match
                (HE) MedicareId = (Ratio9) MedicareId
                and
                (HE) discharge date between (Ratio9) start and end date
                if no matches: (HE) discharge date - 1 year between (Ratio9) start and end date
                if no matches: (HE) discharge date - 2 year between (Ratio9) start and end date
                if no matches: (HE) discharge date - 3 year between (Ratio9) start and end date
        """

        # join rules
        conditions = [
            (
                    (
                            (df2.f_m_flag == "F") & (df1.facility_id == df2.f_m_id)
                    )
                    |
                    (
                            (df2.f_m_flag == "M") & (df1.fac_medicare_id == df2.f_m_id)
                    )
            )
            &
            (
                    (df1.discharge_date.between(df2.start_date, df2.end_date))
                    |
                    (add_months(df1.discharge_date, -12).between(df2.start_date, df2.end_date))
                    |
                    (add_months(df1.discharge_date, -24).between(df2.start_date, df2.end_date))
                    |
                    (add_months(df1.discharge_date, -36).between(df2.start_date, df2.end_date))
            )
        ]

        # join 2 dataframes
        df_joined = df1.join(df2, conditions)
        # dropped unnecessary columns
        df_joined = df_joined.drop("f_m_flag", "f_m_id")

        return df_joined

"""Contains all methods for getting data frames set in the task 2"""

import os

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import when, collect_list, array_contains

from spark_session import Spark
from task5.schemas import TASK5_1_SCHEMA, TASK5_2_SCHEMA
from task5.file_names import TASK5_1, TASK5_2


class DataFrames(Spark):
    """Contains all methods for getting data frames set in the tasks"""

    def __init__(self):
        self.df_task5_1 = None
        self.df_task5_2 = None
        super().__init__()

    def start(self) -> None:
        """Class pipeline. All class methods calling here"""

        self.df_task5_1 = self.get_data_frame(TASK5_1_SCHEMA, TASK5_1)
        self.df_task5_2 = self.get_data_frame(TASK5_2_SCHEMA, TASK5_2)

        self.collect_list()

        df_task5_1_task5_2 = self.join_2_dataframes(self.df_task5_1, self.df_task5_2, "id", "id")
        df_task5_1_task5_2.show()

        self.result = self.count_quarter(df_task5_1_task5_2)

    def get_data_frame(self, schema: StructType, file_name: str) -> DataFrame:
        """Read current csv file(file_name) with schema to data frame. Returns PySpark DataFrame"""

        data_frame = (
            self.spark_session.read  # The DataFrameReader
            .option('sep', ',')  # Use tab delimiter (default is comma-separator)
            .option('header', 'true')  # Use first line of all files as header
            .schema(schema)  # Use the specified schema
            # Creates a DataFrame from CSV after reading in the file
            .csv(os.path.abspath(file_name))
        )
        return data_frame

    def collect_list(self):
        """
        Aggregate the 'month' column into a list
            e.g Row(id=1, month='jun'), Row(id=1, month='sep')
            to Row(id=1, month_list=['jun', 'sep'])
        """

        self.df_task5_2 = (
            self.df_task5_2
            .groupBy("id").agg(collect_list("month").alias("month_list"))
        )

    @staticmethod
    def count_quarter(df):
        """Creates new columns (q1, q2, q3, q4) and fill them according to the conditions"""

        # cond for 1st quarter
        q1_conditions = (
                (df.m1 == 1) | (df.m2 == 1) | (df.m3 == 1) |
                (array_contains("month_list", "jan")) |
                (array_contains("month_list", "feb")) |
                (array_contains("month_list", "mar"))
        )

        # cond for 2nd quarter
        q2_conditions = (
                (df.m4 == 1) | (df.m5 == 1) | (df.m6 == 1) |
                (array_contains("month_list", "apr")) |
                (array_contains("month_list", "may")) |
                (array_contains("month_list", "jun"))
        )

        # cond for 3rd quarter
        q3_conditions = (
                (df.m7 == 1) | (df.m8 == 1) | (df.m9 == 1) |
                (array_contains("month_list", "jul")) |
                (array_contains("month_list", "aug")) |
                (array_contains("month_list", "sep"))
        )

        # cond for 4th quarter
        q4_conditions = (
                (df.m10 == 1) | (df.m11 == 1) | (df.m12 == 1) |
                (array_contains("month_list", "oct")) |
                (array_contains("month_list", "nov")) |
                (array_contains("month_list", "dec"))
        )

        # Creates new columns (q1, q2, q3, q4) and fill them according to the conditions
        df = (
            df
            .withColumn("q1", when(q1_conditions, 1).otherwise(0))
            .withColumn("q2", when(q2_conditions, 1).otherwise(0))
            .withColumn("q3", when(q3_conditions, 1).otherwise(0))
            .withColumn("q4", when(q4_conditions, 1).otherwise(0))
            # remove unused columns
            .drop(
                "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10", "m11", "m12",
                "month_list"
            )
        )
        return df

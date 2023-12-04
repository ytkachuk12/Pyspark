"""
Contains SparkSession obj, abstract method for creation DataFrame,
method for writing data frame to csv file
"""
import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class Spark:
    """Contains SparkSession obj and abstract method for creation DataFrame"""
    parent_dir_path = Path().resolve().parent
    results_dir_path = os.path.join(parent_dir_path, "results")

    def __init__(self):
        self.name = None
        self.spark_session = None

        self.result = None

    @property
    def session(self):
        """Returns SparkSession obj"""
        print("OBJ")
        return self.spark_session

    @session.setter
    def session(self, name: str = "spark_app") -> SparkSession:
        """Set spark obj name and create spark session"""

        self.name = name
        self.spark_session = (
            SparkSession.builder
            .appName(self.name)
            # Sets the Spark master URL to connect to.
            # “local” to run locally, “local[4]” to run with 4 cores
            .master('local')
            .getOrCreate()
        )
        # need for converting "MM/dd/yyyy" string to Date
        self.spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    def get_data_frame(self):
        """Abstract method"""

        raise NotImplementedError("Please Implement this method")

    @staticmethod
    def join_2_dataframes(
            df1: DataFrame, df2: DataFrame, column_name1: str, column_name2: str, *args: str
    ) -> DataFrame:
        """Join 2 dataframes by column names and drop columns passed in args"""

        df_joined = (
            df1
            .join(df2, df1[column_name1] == df2[column_name2], "inner")
            .drop(df2[column_name2])
        )

        if args:
            df_joined = df_joined.drop(*list(args))

        return df_joined

    def save(self, subtask_name: str = None):
        """Write data_frame to csv"""

        result_dir_path = os.path.join(self.results_dir_path, self.name)
        if subtask_name:
            result_dir_path = os.path.join(result_dir_path, subtask_name)

        if os.path.exists(result_dir_path) and os.path.isdir(result_dir_path):
            shutil.rmtree(result_dir_path)

        (self.result
         .write
         .option("header", True)
         .csv(result_dir_path)
         )
        return True

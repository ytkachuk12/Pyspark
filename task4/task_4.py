"""Contains all methods for getting data frames set in the tasks"""

import os

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, max, desc, count
from pyspark.sql import Window

from spark_session import Spark
from task4.file_names import TASK4_DATA
from task4.schemas import TASK4_DATA_SCHEMA


class DataFrames(Spark):
    """Contains all methods for getting data frames set in the tasks"""

    def __init__(self):

        self.df_task4 = None
        super().__init__()

    def start(self) -> None:
        """Class pipeline. All class methods calling here"""

        # get incoming data
        self.df_task4 = self.get_data_frame()

        # Get the most popular(frequent) value for columns 'fac_prof' and 'proc_code'
        df_top_fac_prof = self.max_value_in_column("fac_prof", "proc_code")
        df_top_proc_code = self.max_value_in_column("proc_code", "fac_prof")

        # join 2 dataframes
        self.result = self.join_2_dataframes(
            df_top_fac_prof, df_top_proc_code, "patient_id", "patient_id"
        )

    def get_data_frame(self) -> DataFrame:
        """Read current csv file(file_name) with schema to data frame. Returns PySpark DataFrame"""

        data_frame = (
            self.spark_session.read  # The DataFrameReader
            .option('sep', ',')  # Use tab delimiter (default is comma-separator)
            .option('header', 'true')  # Use first line of all files as header
            .schema(TASK4_DATA_SCHEMA)  # Use the specified schema
            # Creates a DataFrame from CSV after reading in the file
            .csv(os.path.abspath(TASK4_DATA))
        )

        return data_frame

    def max_value_in_column(self, column_name, dropped_column_name) -> DataFrame:
        """Get the most popular(frequent) value for the given column(arg: column_name)"""

        # partition df by id and column_name("fac_prof" or "proc_code")
        window = Window().partitionBy(self.df_task4["patient_id"], self.df_task4[column_name])
        # partition df by id.
        window_max = Window().partitionBy(self.df_task4["patient_id"])

        # we work with "fac_prof" or "proc_code" column.
        # the second one(arg: dropped_column_name) is not needed and therefore dropped
        data_frame = self.df_task4.drop(dropped_column_name)

        # counts how many values of column_name("fac_prof" or "proc_code") for each patient_id
        # and counts maximum(frequent) column_name("fac_prof" or "proc_code")
        data_frame = (
            data_frame
            .withColumn("count", count(col(column_name)).over(window))
            .distinct()
            .withColumn("max(count)", max(col("count")).over(window_max))
        )

        # filter only max values and drop unnecessary(intermediate) columns
        data_frame = (
            data_frame
            .filter(col("max(count)") == col("count"))
            .drop("count", "max(count)")
        )

        return data_frame

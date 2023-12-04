"""Contains all methods for getting data frames set in the task 2"""

import os
from datetime import datetime
# pip install python-dateutil
from dateutil.relativedelta import relativedelta

from pyspark.sql.functions import col, unix_timestamp, collect_list, array_contains, when

from spark_session import Spark
from task6.schemas import ENROLL_SCHEMA
from task6.file_names import ENROLL


class DataFrames(Spark):
    """Contains all methods for getting data frames set in the tasks"""

    def __init__(self):
        self.df_enroll = None
        self.end_date = datetime.strptime("2016-09-01", '%Y-%m-%d')

        super().__init__()

    def start(self) -> None:
        """Class pipeline. All class methods calling here"""

        self.get_enroll()
        self.collect_list()
        self.count_continuously_visited_hospital()

        self.result = self.df_enroll

    def get_enroll(self) -> None:
        """
        Read the 'enroll' csv file with schema to data frame. Returns PySpark DataFrame
        """

        self.df_enroll = (
            self.spark_session.read  # The DataFrameReader
            .option('sep', ',')  # set delimiter
            .option('header', 'true')  # Use first line of all files as header
            .schema(ENROLL_SCHEMA)
            # Creates a DataFrame from CSV after reading in the file
            .csv(os.path.abspath(ENROLL))
            # convert string "MMddyyyy" to Date
            .withColumn("effective_from_date",
                        unix_timestamp(col("effective_from_date"), "MMddyyyy").cast("timestamp"))
        )

    def collect_list(self) -> None:
        """
        Aggregate the 'effective_from_date' column into a list
            e.g Row(id=1, effective_from_date='2015-01-01') &
                Row(id=1, effective_from_date='2015-02-01')
            to Row(id=1, effective_from_date=['2015-01-01', '2015-01-01'])
        """

        self.df_enroll = (
            self.df_enroll
            .groupBy("patient_id")
            .agg(collect_list("effective_from_date").alias("effective_from_date_list"))
        )

    def count_continuously_visited_hospital(self) -> None:
        """
        Creates new columns (5months, 9months, 11months) and fill them according to the conditions
        """

        # cond for count continuously visited hospital last 5 months
        conditions_5 = (
                (array_contains("effective_from_date_list",
                                self.end_date)) &
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=1))) &
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=2))) &
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=3))) &
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=4)))
        )

        # cond for count continuously visited hospital last 9 months
        conditions_9 = conditions_5 & (
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=5))) &
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=6))) &
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=7))) &
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=8)))
        )

        # cond for count continuously visited hospital last 11 months
        conditions_11 = conditions_9 & (
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=9)))
                &
                (array_contains("effective_from_date_list",
                                self.end_date - relativedelta(months=10)))
        )

        # Creates new columns (5months, 9months, 11months) and fill them according to the conditions
        self.df_enroll = (
            self.df_enroll
            .withColumn("5months", when(conditions_5, True).otherwise(False))
            .withColumn("9months", when(conditions_9, True).otherwise(False))
            .withColumn("11months", when(conditions_11, True).otherwise(False))
            .drop("effective_from_date_list")
        )

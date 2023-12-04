"""Contains all methods for getting data frames set in the task 2"""

import os

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, array, arrays_zip, explode, unix_timestamp, sum

from spark_session import Spark
from task2.schemas import HOSPITAL_ENCOUNTER_SCHEMA, REVTOBUCKET_SCHEMA
from task2.file_names import HOSPITAL_ENCOUNTER, REVTOBUCKET


class DataFrames(Spark):
    """Contains all methods for getting data frames set in the tasks"""

    def __init__(self):

        self.df_hospital_encounter = None
        self.df_revtobucket = None
        super().__init__()

    def start(self) -> None:
        """Class pipeline. All class methods calling here"""

        self.df_hospital_encounter = self.get_hospital_encounter()
        self.df_revtobucket = self.get_revtobucket()
        df_explode_hospital_encounter = self.explode_code_chg(self.zip_code_chg())
        df_hospital_encounter_Revtobucket = self.join_2_dataframes(
            df_explode_hospital_encounter, self.df_revtobucket, "rev_code", "rev_code", "id_bucket"
        )
        self.result = self.aggregate_rev_code(df_hospital_encounter_Revtobucket)

    def get_hospital_encounter(self) -> DataFrame:
        """
        Read the 'hospital_encounter' csv file with schema to data frame. Returns PySpark DataFrame
        """

        df_hospital_encounter = (
            self.spark_session.read  # The DataFrameReader
            .option('sep', ',')  # set delimiter
            .option('header', 'true')  # Use first line of all files as header
            .schema(HOSPITAL_ENCOUNTER_SCHEMA)
            # Creates a DataFrame from CSV after reading in the file
            .csv(os.path.abspath(HOSPITAL_ENCOUNTER))
            # convert string "MMddyyyy" to Date
            .withColumn("discharge_date",
                        unix_timestamp(col("discharge_date"), "MMddyyyy").cast("timestamp"))
        )
        return df_hospital_encounter

    def get_revtobucket(self) -> DataFrame:
        """
        Read the 'revtobucket' csv file with schema to data frame. Returns PySpark DataFrame
        """

        df_revtobucket = (
            self.spark_session.read  # The DataFrameReader
            .option('header', 'false')  # Use first line of all files as header
            .option('sep', ';')  # Use ';' delimiter (default is comma-separator)
            .option("timestampFormat", "MM/dd/yyyy")  # set Date format
            .schema(REVTOBUCKET_SCHEMA)  # Use the specified schema
            # Creates a DataFrame from CSV after reading in the file
            .csv(os.path.abspath(REVTOBUCKET))
        )
        return df_revtobucket

    @property
    def rev_code_array(self):
        """Create array from rev_code(1-50) columns"""

        rev_code_array = array(
            col("rev_code1"), col("rev_code2"), col("rev_code3"), col("rev_code4"),
            col("rev_code5"), col("rev_code6"), col("rev_code7"), col("rev_code8"),
            col("rev_code9"), col("rev_code10"), col("rev_code11"), col("rev_code12"),
            col("rev_code13"), col("rev_code14"), col("rev_code15"), col("rev_code16"),
            col("rev_code17"), col("rev_code18"), col("rev_code19"), col("rev_code20"),
            col("rev_code21"), col("rev_code22"), col("rev_code23"), col("rev_code24"),
            col("rev_code25"), col("rev_code26"), col("rev_code27"), col("rev_code28"),
            col("rev_code29"), col("rev_code30"), col("rev_code31"), col("rev_code32"),
            col("rev_code33"), col("rev_code34"), col("rev_code35"), col("rev_code36"),
            col("rev_code37"), col("rev_code38"), col("rev_code39"), col("rev_code40"),
            col("rev_code41"), col("rev_code42"), col("rev_code43"), col("rev_code44"),
            col("rev_code45"), col("rev_code46"), col("rev_code47"), col("rev_code48"),
            col("rev_code49"), col("rev_code50")
        ).alias("rev_code_array")
        return rev_code_array

    @property
    def chg_array(self):
        """Create array from chg(1-50) columns"""

        chg_array = array(
            col("chg1"), col("chg2"), col("chg3"), col("chg4"), col("chg5"),
            col("chg6"), col("chg7"), col("chg8"), col("chg9"), col("chg10"),
            col("chg11"), col("chg12"), col("chg13"), col("chg14"), col("chg15"),
            col("chg16"), col("chg17"), col("chg18"), col("chg19"), col("chg20"),
            col("chg21"), col("chg22"), col("chg23"), col("chg24"), col("chg25"),
            col("chg26"), col("chg27"), col("chg28"), col("chg29"), col("chg30"),
            col("chg31"), col("chg32"), col("chg33"), col("chg34"), col("chg35"),
            col("chg36"), col("chg37"), col("chg38"), col("chg39"), col("chg40"),
            col("chg41"), col("chg42"), col("chg43"), col("chg44"), col("chg45"),
            col("chg46"), col("chg47"), col("chg48"), col("chg49"), col("chg50"),
        ).alias("chg_array")
        return chg_array

    def zip_code_chg(self) -> DataFrame:
        """Zip 2 arrays: rev_code and chg.([{rev_code1, chg1},{null,null},...])"""

        data_frame = (
            self.df_hospital_encounter.select(
                arrays_zip(self.rev_code_array, self.chg_array).alias("zipped_code_chg"),
                col("discharge_date")
            )
        )
        return data_frame

    @staticmethod
    def explode_code_chg(data_frame: DataFrame) -> DataFrame:
        """Explode [rev_code, chg] array to strings"""

        data_frame = (
            data_frame
            .select(explode("zipped_code_chg").alias("exploded_code_chg"), col("discharge_date"))

        )
        data_frame = (
            data_frame
            .select(
                (data_frame["exploded_code_chg"]["rev_code_array"]).alias("rev_code"),
                (data_frame["exploded_code_chg"]["chg_array"]).alias("service cost"),
                data_frame["discharge_date"],
            )
            .filter(col("rev_code").isNotNull())
        )
        return data_frame

    @staticmethod
    def aggregate_rev_code(data_frame: DataFrame) -> DataFrame:
        """
        Filter by discharge_date.
        Group by 'rev_code' and count sum 'service cost'(previous chg column) for each rev_code
        """

        data_frame = data_frame.filter(
            col('discharge_date').between(col("start_date"), col('end_date'))
        )
        data_frame = (
            data_frame
            .groupBy(col("rev_code"))
            .agg(sum(col("service cost")))
            .orderBy(col("rev_code"))
        )
        return data_frame

"""Contains all methods for getting data frames set in the tasks"""

import os
from datetime import datetime

from pyspark.sql.dataframe import DataFrame, StructType
from pyspark.sql.functions import desc, row_number, col, floor, lit, concat, count
from pyspark.sql import Window

from spark_session import Spark


class DataFrames(Spark):
    """Contains all methods for getting data frames set in the tasks"""

    def get_data_frame(self, schema: StructType, file_name: str) -> DataFrame:
        """Read current csv file(file_name) with schema to data frame. Returns PySpark DataFrame"""

        data_frame = (
            self.spark_session.read  # The DataFrameReader
            .option('sep', '\t')  # Use tab delimiter (default is comma-separator)
            .option('header', 'true')  # Use first line of all files as header
            .schema(schema)  # Use the specified schema
            # Creates a DataFrame from CSV after reading in the file
            .csv(os.path.abspath(file_name))
        )
        return data_frame

    @ staticmethod
    def movies_lot_votes(data_frame: DataFrame) -> DataFrame:
        """Find movies (only movies) that have more than 100,000 votes"""

        data_frame = (
            data_frame
            .filter(data_frame.votes > 100000)
            .filter(data_frame.type == "movie")
        )
        return data_frame

    @staticmethod
    def top100(data_frame: DataFrame) -> DataFrame:
        """Returns top 100 rated movies"""

        data_frame = (
            data_frame
            .sort(data_frame["average rating"].desc())
        )
        return data_frame

    @staticmethod
    def top100_last_10years(data_frame: DataFrame) -> DataFrame:
        """Returns top 100 rated movies for last 10 years"""

        last10_years = datetime.now().year - 10  # get last 10 years from now

        data_frame = (
            data_frame
            .filter(data_frame["start year"] >= last10_years)
            .sort(data_frame["average rating"].desc())
        )
        return data_frame

    @staticmethod
    def top100_in1960s(data_frame: DataFrame) -> DataFrame:
        """Returns top 100 rated movies produced in the 1960s"""

        data_frame = (
            data_frame
            .filter(data_frame["start year"].between(1960, 1969))
            .sort(data_frame["average rating"].desc())
        )
        return data_frame

    @staticmethod
    def top10_of_genre(data_frame: DataFrame) -> DataFrame:
        """Returns the top 10 films of each genre"""

        window = Window().partitionBy(data_frame["genres"]).orderBy(desc("average rating"))
        data_frame = (
            data_frame
            .withColumn("row number", row_number().over(window))
            .filter(col("row number") <= 10)
        )
        return data_frame

    @staticmethod
    def top10_of_genre_decade(data_frame: DataFrame) -> DataFrame:
        """Returns the top 10 films of each genre for each 10 years from 1950"""

        start_year = 1950

        data_frame = (
            data_frame.withColumn("decade", concat(
                (floor(col("start year") / 10) * 10), lit("-"),
                (floor(col("start year") / 10) * 10) + 10
            ))
            .filter(data_frame["start year"] >= start_year)
        )

        window = (Window().partitionBy(
            data_frame["decade"], data_frame["genres"]).orderBy("decade", "genres")
        )
        data_frame = (
            data_frame
            .withColumn("row", row_number().over(window))
            .filter(col("row") <= 10)
            .sort("decade")
        )
        return data_frame

    @staticmethod
    def top_actors(data_frame: DataFrame) -> DataFrame:
        """Returns the actors and actress participating in top-rated movies"""

        data_frame = data_frame.filter(
            (data_frame.category == "actor") | (data_frame.category == "actress")
        )
        window = Window().partitionBy(data_frame["name"]).orderBy("name")
        data_frame = (
            data_frame
            .withColumn("partition in top movies", count("name").over(window))
            .select("name").distinct()
            .sort(col("partition in top movies").desc())
        )
        return data_frame

    @staticmethod
    def top5_for_directors(data_frame: DataFrame) -> DataFrame:
        """Returns the top 5 films for each director(producer)"""

        window = Window().partitionBy(data_frame["name"]).orderBy("name")
        data_frame = (
            data_frame
            .withColumn("row number", row_number().over(window))
            .filter(col("row number") <= 5)
            .sort(desc("average rating"))
        )
        return data_frame

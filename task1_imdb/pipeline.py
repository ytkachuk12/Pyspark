"""All pipeline steps is here"""

from pyspark.sql.functions import explode, split

from spark_session import write_to_file
from task1_imdb.task1 import DataFrames
from task1_imdb.file_names import (
    NAME_BASICS, TITLE_BASICS, TITLE_PRINCIPALS, TITLE_CREW, TITLE_RATINGS
)
from task1_imdb.schemas import (
    NAME_SCHEMA,
    TITLE_BASICS_SCHEMA,
    TITLE_PRINCIPALS_SCHEMA,
    TITLE_RATINGS_SCHEMA,
    TITLE_CREW_SCHEMA
)

# Create SparkSession obj
df = DataFrames()
df.session = "task1"


# create data frames from csv files
df_name = df.get_data_frame(NAME_SCHEMA, NAME_BASICS)

df_title_basics = df.get_data_frame(TITLE_BASICS_SCHEMA, TITLE_BASICS)

df_title_principals = df.get_data_frame(TITLE_PRINCIPALS_SCHEMA, TITLE_PRINCIPALS)

df_ratings = df.get_data_frame(TITLE_RATINGS_SCHEMA, TITLE_RATINGS)

df_title_crew = df.get_data_frame(TITLE_CREW_SCHEMA, TITLE_CREW)
df_title_crew = df_title_crew.withColumn("directors", explode(split(df_title_crew.directors, ",")))

# join 2 df
df_title_basics_Ratings = df.join_2_dataframes(
    df_title_basics, df_ratings, "id", "id", "runtime minutes", "end year"
)
# get movies with votes more than 100k and explode it by genres
df_movies_lot_votes = df.movies_lot_votes(df_title_basics_Ratings)
df_exploded_movies_lot_votes = df_movies_lot_votes.withColumn(
    "genres", explode(split(df_movies_lot_votes.genres, ","))
)

# task1. top 100
top100 = df.top100(df_movies_lot_votes)
top100_last_10years = df.top100_last_10years(df_movies_lot_votes)
top100_1960s = df.top100_in1960s(df_movies_lot_votes)
write_to_file(top100, "top100.csv")
write_to_file(top100_last_10years, "top100_last_10years.csv")
write_to_file(top100_1960s, "top100_1960s.csv")

# task2. top 10 of genre
write_to_file(df.top10_of_genre(df_exploded_movies_lot_votes), "top10_of_genre.csv")

# task3. top 10 of genre for each 10 years from 1950
write_to_file(df.top10_of_genre_decade(df_exploded_movies_lot_votes), "top10_of_genre_decade.csv")

# task 4. top actors
# join 2 dataframes: title_principals and name
df_title_principals_Name = df.join_2_dataframes(
    df_title_principals, df_name, "person id", "id",
    "ordering", "job", "characters",
    "year of birth", "year of death", "known for titles id's",  # "primary profession",
)
# join 2 dataframes: title_principals_name and movies_lot_votes
df_title_principals_Name_Movies_lot_votes = df.join_2_dataframes(
    df_movies_lot_votes, df_title_principals_Name, "id", "id",
    "is adult", "array genres"
)
write_to_file(df.top_actors(df_title_principals_Name_Movies_lot_votes), "top_actors.csv")

# task 5. top5 movies for each director
# join 2 dataframes: title_basics_Ratings and title_crew
df_title_basics_Ratings_Crew = df.join_2_dataframes(
    df_movies_lot_votes, df_title_crew, "id", "id",
    "original title", "is adult", "array genres", "writers"
)
# join 2 dataframes: df_title_basics_Ratings_Crew and name
df_name_Title_basics_Ratings_Crew = df.join_2_dataframes(
    df_name, df_title_basics_Ratings_Crew, "id", "directors",
    "id", "year of birth", "year of death", "primary profession", "known for titles id's"
)
write_to_file(df.top5_for_directors(df_name_Title_basics_Ratings_Crew), "top5_for_directors.csv")

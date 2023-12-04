"""Py Spark Schemas (StructType) for data files"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ByteType, FloatType
)

# Contains the following information for names:
NAME_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("year of birth", IntegerType(), False),
        StructField("year of death", IntegerType(), True),
        StructField("primary profession", StringType(), False),
        StructField("known for titles id's", StringType(), True)
    ]
)

# Contains the information for titles used by the filmmakers(original)
TITLE_BASICS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        # (e.g. movie, short, tvseries, tvepisode, video, etc)
        StructField("type", StringType(), False),
        StructField("primary title", StringType(), False),  # the title used by the filmmakers
        # original title, in the original language
        StructField("original title", StringType(), False),
        StructField("is adult", ByteType(), False),
        StructField("start year", IntegerType(), True),
        StructField("end year", IntegerType(), True),
        StructField("runtime minutes", IntegerType(), True),
        StructField("genres", StringType(), True)
    ]
)

# Contains the information for localized titles.
TITLE_AKAS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("ordering", IntegerType(), False),
        StructField("localized title", StringType(), False),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("is original title", ByteType(), True)
    ]
)

# Contains the principal cast/crew for titles
TITLE_PRINCIPALS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("ordering", IntegerType(), False),
        StructField("person id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ]
)

# Contains the IMDb rating and votes information for titles
TITLE_RATINGS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("average rating", FloatType(), False),
        StructField("votes", IntegerType(), False)
    ]
)

# Contains the director and writer information for all the titles in IMDb
TITLE_CREW_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True)
    ]
)

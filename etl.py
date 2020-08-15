import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, udf
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

# noinspection PyTypeChecker
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['ACCESS_KEY_ID']
# noinspection PyTypeChecker
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET_ACCESS_KEY']


def get_song_src_schema():
    """
    Get the source spark schema definition
    :return: The schema definition
    """
    return R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
    ])


def get_log_src_schema():
    """
    Get the source spark schema definition
    :return: The schema definition
    """

    return R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Int()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Str()),
        Fld("sessionId", Int()),
        Fld("song", Str()),
        Fld("status", Int()),
        Fld("ts", Str()),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])


def get_song_src_field_exprs():
    """
    Get the array of fields used in the songs table
    :return: Array of songs table fields
    """
    return ["song_id", "title", "artist_id", "year", "duration"]


def get_song_dst_partitions():
    """
    Get the ordered array of fields used to partition the songs table
    :return: Ordered array of partition fields
    """
    return ["year", "artist_id"]


def get_time_dst_partitions():
    """
    Get the ordered array of fields used to partition the time table
    :return: Ordered array of partition fields
    """
    return ["year", "month"]


def get_artist_src_field_exprs():
    """
    Get the array of fields used in the artists table
    :return: Array of artists table fields
    """
    return ["artist_id", "artist_name as artist", "artist_latitude as latitude", "artist_longitude as longitude",
            "artist_location as location"]


def get_user_table_exprs():
    """
    Get the select expression for the users table
    :return: User table select expression array
    """

    return ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]


def get_songplays_dst_exprs():
    """
    Get the select expression for the songplays table
    :return: Songplays table select expression array
    """
    return ["uuid() as songplay_id", "ts as start_time", "userId as user_id", "level", "song_id", "artist_id",
            "location", "userAgent as user_agent", "played_year", "month as played_month"]


def get_songplays_dst_partitions():
    """
    Get the ordered partition array for the songplays table
    :return: Ordered songplays table partition array
    """
    return ["played_year", "played_month"]


def create_spark_session():
    """
    create spark session singleton
    :return: spark session singleton
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str):
    """
    Load from source, process into parquet files, and write to the destination
    :param spark: Spark session singleton
    :param input_data: The location URL for the song data's root folder
    :param output_data: The destination URL
    """
    src_schema = get_song_src_schema()
    df = spark.read.schema(src_schema).json(input_data)

    song_exprs = get_song_src_field_exprs()
    write_partition = get_song_dst_partitions()

    songs_table = df.selectExpr(*song_exprs)
    songs_table = songs_table.dropDuplicates()
    songs_table.repartition(*write_partition).write.partitionBy(*write_partition).mode('overwrite').parquet(
        f"{output_data}/songs/")

    artist_exprs = get_artist_src_field_exprs()
    artists_table = df.selectExpr(*artist_exprs)
    artists_table = artists_table.dropDuplicates()
    artists_table.write.mode('overwrite').parquet(f"{output_data}/artists/")


def format_datetime(ts):
    """
    Truncate time from epoch in milliseconds to time from epoch in seconds.
    :return: Truncated timestamp
    """
    return datetime.fromtimestamp(ts / 1000.0)


def process_log_data(spark: SparkSession, input_data: str, output_data: str, song_data: str):
    """
    Load from source, process into parquet files, and write to the destination
    :param spark: The Spark session singleton
    :param input_data: The location URL for the log data's root folder
    :param output_data: The destination URL
    :param song_data: The src location for song data files
    """

    src_schema = get_log_src_schema()
    logs_df = spark.read.schema(src_schema).json(input_data)
    logs_df = logs_df.where("page = 'NextSong'")

    user_exprs = get_user_table_exprs()
    users_table = logs_df.selectExpr(*user_exprs).distinct()
    users_table.write.mode('overwrite').parquet(f"{output_data}/users/")
    print("Write users table complete")

    get_timestamp = udf(lambda x: format_datetime(int(x)), TimestampType())

    logs_df = logs_df.withColumn("ts_timestamp", get_timestamp(logs_df.ts))

    time_table = logs_df.select(
        "ts_timestamp",
        hour("ts_timestamp").alias("hour"),
        dayofmonth("ts_timestamp").alias("day"),
        weekofyear("ts_timestamp").alias("week"),
        month("ts_timestamp").alias("month"),
        year("ts_timestamp").alias("year"),
        date_format("ts_timestamp", "EEEE").alias("weekday")
    ).distinct()

    time_partitions = get_time_dst_partitions()
    time_table.repartition(*time_partitions).write.partitionBy(*time_partitions).mode('overwrite').parquet(
        f"{output_data}/time/")

    song_df = spark.read.parquet(song_data)
    songplay_exprs = get_songplays_dst_exprs()
    songplay_partitions = get_songplays_dst_partitions()

    # noinspection PyTypeChecker
    songplays_table = logs_df.join(song_df, logs_df.song == song_df.title)
    songplays_table.show(5)
    time_table = time_table.selectExpr("ts_timestamp", "month", "year as played_year")
    # noinspection PyTypeChecker
    songplays_table = songplays_table.join(time_table, songplays_table.ts_timestamp == time_table.ts_timestamp)
    songplays_table.show(5)
    songplays_table = songplays_table.selectExpr(*songplay_exprs)
    songplays_table.show(5)

    songplays_table.repartition(*songplay_partitions).write.partitionBy(*songplay_partitions).mode("overwrite").parquet(
        f"{output_data}/songplays/"
    )


# noinspection SpellCheckingInspection
def main():
    """
    Run the ETL process
    """
    output_data = "s3a://data-engineering-data-lake/analytics/"
    songs_folder = "s3a://udacity-dend/song-data/*/*/*/*.json"
    logs_folder = "s3a://udacity-dend/log-data/*/*/*.json"
    songs_parquet_input = f"{output_data}/songs/"

    spark = create_spark_session()
    process_song_data(spark, songs_folder, output_data)
    process_log_data(spark, logs_folder, output_data, songs_parquet_input)
    spark.stop()


if __name__ == "__main__":
    main()

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    '''
    create spark session
    :return: spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    process song data.
    :param spark: spark session
    :param input_data: input data directory
    :param output_data: output data directory
    '''
    # get filepath to song data file
    song_data = f'{input_data}song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.filter(df.song_id.isNotNull()).select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(f'{output_data}/songs.parquet')

    # extract columns to create artists table
    artists_table = df.filter(df.artist_id.isNotNull()).select(
            col('artist_id'),
            col('artist_name').alias('name'),
            col('artist_location').alias('location'),
            col('artist_latitude').alias('latitude'),
            col('artist_longitude').alias('longitude')
        ).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(f'{output_data}/artists.parquet')


def process_log_data(spark, input_data, output_data):
    '''
    process log data.
    :param spark: spark session
    :param input_data: input data directory
    :param output_data: output data directory
    '''
    # get filepath to log data file
    log_data = f'{input_data}log_data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'),
        col('lastName').alias('last_name'),
        col('gender'),
        col('level')
    ).distinct()
    
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(f'{output_data}/users.parquet')

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_datetime(col("ts")))
   
    # extract columns to create time table
    time_table =  df.withColumn("hour", hour(col("start_time"))) \
            .withColumn("day", dayofmonth(col("start_time"))) \
            .withColumn("week", weekofyear(col("start_time"))) \
            .withColumn("month", month(col("start_time"))) \
            .withColumn("year", year(col("start_time"))) \
            .withColumn("weekday", dayofweek(col("start_time"))) \
            .select(
                col("start_time"),
                col("hour"),
                col("day"),
                col("week"),
                col("month"),
                col("year"),
                col("weekday")
            ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(f'{output_data}/time.parquet')
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(f'{output_data}/songs.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.title == df.song) \
                        .withColumn("sonplay_id", monotonically_increasing_id()) \
                        .select(
                            col("sonplay_id"),
                            col("start_time"),
                            col("userId").alias("user_id"),
                            "level",
                            "song_id",
                            "artist_id",
                            col("sessionId").alias("session_id"),
                            "location",
                            col("userAgent").alias("user_agent")
                        )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(f'{output_data}/songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://njade-sparkify"
                                        
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.close()


if __name__ == "__main__":
    main()

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("source_songs_table")

    # extract columns to create songs table
    songs_table = spark.sql(
        """
        SELECT song_id, title, artist_id, year, duration
        FROM source_songs_table
        WHERE song_id IS NOT NULL
        ON CONFLICT (song_id) 
        DO NOTHING;
        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql(
        """
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM source_songs_table
        WHERE artist_id IS NOT NULL
        ON CONFLICT (artist_id) 
        DO NOTHING;
        """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("source_logs_table")
    
    # filter by actions for song plays
    df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_table = spark.sql(
        """
        select distinct userId as user_id, firstName as first_name, lastName as last_name, gender, level
        from source_logs_table 
        where userId is not null
        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # extract columns to create time table
    time_table = spark.sql(
        """ 
        SELECT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time_insert,
        DATE_PART(hrs, start_time_insert) as hours,
        DATE_PART(dayofyear, start_time_insert) as day,
        DATE_PART(w, start_time_insert) as week,
        DATE_PART(mons ,start_time_insert) as month,
        DATE_PART(yrs , start_time_insert) as year,
        DATE_PART(dow, start_time_insert) as day_of_week
        FROM source_logs_table;
        """)
    
    # write time table to parquet files
    time_table.write.mode('overwrite').parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    song_df = spark.sql(
        """
        SELECT song_id, title, artist_id, year, duration
        FROM source_logs_table
        WHERE song_id IS NOT NULL
        ON CONFLICT (song_id) 
        DO NOTHING;
        """
        )

    # write time table to parquet files
    song_df.write.mode('overwrite').parquet(output_data+'song_df/')

    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(
        """
        SELECT DISTINCT DATE_ADD('ms', se.ts, '1970-01-01') AS start_time, 
        source_logs_table.user_id,
        source_logs_table.level,
        source_songs_table.song_id,
        source_songs_table.artist_id,
        source_logs_table.session_id,
        source_logs_table.location,
        source_logs_table.user_agent
        FROM source_logs_table 
        JOIN source_songs_table ON source_logs_table.song = source_songs_table.title AND source_logs_table.artist = source_songs_table.artist_name
        WHERE source_logs_table.page = 'NextSong'
        AND source_logs_table.user_id IS NOT NULL
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://my_s3/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

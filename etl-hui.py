import configparser
import datetime 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS",'AWS_ACCESS_KEY_ID') 
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS",'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    The Function creates a spark session
    Args:
    None

    Returns:
    spark sql session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark):
    """
    The Function reads in song data and create it as a temp view, then
    query the table to produce two dimensional tables and save them
    in parquet format
    Args:
    spark

    Returns:
    None
    """
    
    # get filepath to song data file
    song_files ="s3a://udacity-dend/song_data/*/*/*/"
    
    # read song data file

    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("year",Int()),
            ])
    song_data = spark.read.json(song_files, schema=songSchema)
    
    # create a temp view
    song_data.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    song_table = spark.sql("""
            SELECT distinct song_id, 
                   title, 
                   artist_id, 
                   int(year) as year, 
                   float(duration) as duration
              from song_data
              order by year, artist_id
            """)
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.partitionBy("year","artist_id").parquet("s3a://udacity-dend/SongDB/song_table")

    # extract columns to create artists table
    artists_table = spark.sql("""
              SELECT distinct artist_id, 
                     artist_name as name, 
                     artist_location as location, 
                     float(artist_latitude) as latitude, 
                     float(artist_longitude) as longitude
                from song_data
            """)
    
    # write artists table to parquet files
    artists_table.write.parquet("s3a://udacity-dend/SongDB/artists_table")


def process_log_data(spark):
    """
    The Function reads in log data and create it as a temp view, then
    query the table to produce two dimensional tables and save them
    in parquet format
    Args:
    spark

    Returns:
    None
    """
    # get filepath to log data file
    log_file ="s3a://udacity-dend/log_data/*/*/*.json"

    # read log data file
    log_data = spark.read.json(log_file)
    
    # filter by actions for song plays
    df = log_data.filter(log_data.page=="NextSong")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # create a temp view
    df.createOrReplaceTempView("log_data")
    
    # extract columns to create time table
    time_table = spark.sql("""
                 SELECT distinct start_time,
                        hour(start_time) as hour   ,
                        dayofmonth(start_time) as day,
                        weekofyear(start_time) as week,
                        month(start_time) as month,
                        year(start_time) as year, 
                        dayofweek(start_time) as weekday
                   from log_data
                   order by year, month
            """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet("s3a://udacity-dend/SongDB/time_table")   
    

    # extract columns for users table    
    user_table = spark.sql("""
                SELECT distinct int(userId) as user_id, 
                      firstName as first_name, 
                      lastName as last_name, 
                      gender,
                      level
                 from log_data
                          """)
    
    # write users table to parquet files
    user_table.write.parquet("s3a://udacity-dend/SongDB/user_table")


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
          select distinct monotonically_increasing_id() as songplay_id , 
                l.start_time, 
                l.userId as user_id, 
                l.level, 
                s.song_id, 
                a.artist_id, 
                l.sessionId as session_id, 
                l.location, 
                l.userAgent as user_agent
           from log_data l
           left join song_data s on l.song=s.title
           left join song_data a on l.artist=a.artist_name
           order by year(l.start_time), month(l.start_time)
            """)

    dt = col("start_time").cast("date")
    fname = [(year, "year"), (month, "month")]
    exprs = [col("*")] + [f(dt).alias(name) for f, name in fname]
    
    # write songplays table to parquet files partitioned by year and month
    (songplays_table 
                .select(*exprs) 
                .write
                .partitionBy(*(name for _, name in fname))
                .format("parquet")
                .save("data/output/songplays"))


def main():
    spark = create_spark_session()

    process_song_data(spark)    
    process_log_data(spark)


if __name__ == "__main__":
    main()

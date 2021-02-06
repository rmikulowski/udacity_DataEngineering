import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events_table (
stagingEvents_id integer IDENTITY(0,1),
artist varchar,
auth varchar,
first_name varchar,
gender varchar,
item_in_session integer,
last_name varchar,
length decimal,
level varchar,
location varchar,
method varchar,
page varchar,
registration bigint,
session_id integer,
song varchar,
status integer,
ts bigint,
user_agent varchar,
user_id int
);
""")


staging_songs_table_create = ("""
CREATE TABLE staging_songs_table (
stagingSongs_id integer IDENTITY(0,1),
num_songs integer,
artist_id varchar,
artist_latitude numeric,
artist_longitude numeric,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration numeric,
year integer);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id integer IDENTITY(0,1), 
start_time timestamp, 
user_id int NOT NULL, 
level varchar, 
song_id varchar,  
artist_id varchar,  
session_id int NOT NULL, 
location varchar,  
user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY NOT NULL, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY NOT NULL, 
title varchar, 
artist_id varchar, 
year int, 
duration numeric
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS  artists (
artist_id varchar PRIMARY KEY NOT NULL, 
name varchar, 
location varchar, 
latitude numeric, 
longitude numeric
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY NOT NULL, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int
);
""")

# STAGING TABLES

staging_events_copy = """
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    compupdate off 
    region 'us-west-2'
    json as {};
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = """
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    region 'us-west-2';
""".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent
)
SELECT DISTINCT DATE_ADD('ms', se.ts, '1970-01-01') AS start_time, 
       st_events.user_id,
       st_events.level,
       st_songs.song_id,
       st_songs.artist_id,
       st_events.session_id,
       st_events.location,
       st_events.user_agent
FROM staging_events_table st_events
JOIN staging_songs_table st_songs ON st_events.song = st_songs.title AND st_events.artist = st_songs.artist_name
WHERE st_events.page = 'NextSong'
AND st_events.user_id IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users (
user_id,
first_name,
last_name,
gender,
level
)               
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events_table
WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (
song_id,
title,
artist_id,
year,
duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs_table
WHERE song_id IS NOT NULL
ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
artist_id,
name,
location,
latitude,
longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs_table
WHERE artist_id IS NOT NULL
ON CONFLICT (artist_id) 
DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (
start_time,
hour,
day,
week,
month,
year,
weekday)
SELECT timestamp 'epoch' + st_events.ts/1000 * interval '1 second' as start_time_insert,
DATE_PART(hrs, start_time_insert) as hours,
DATE_PART(dayofyear, start_time_insert) as day,
DATE_PART(w, start_time_insert) as week,
DATE_PART(mons ,start_time_insert) as month,
DATE_PART(yrs , start_time_insert) as year,
DATE_PART(dow, start_time_insert) as day_of_week
FROM staging_events_table st_events;
""")

# QUERY LISTS

create_sTable_queries = [staging_events_table_create, staging_songs_table_create]
create_pTable_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_sTable_queries = [staging_events_table_drop, staging_songs_table_drop]
drop_pTable_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

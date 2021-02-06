# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY,
start_time timestamp NOT NULL,
user_id int NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id int NOT NULL,
location varchar,
user_agent varchar
)
""") # maybe add: UNIQUE (songplay_id, start_time, user_id)

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int NOT NULL UNIQUE,
first_name varchar,
last_name varchar,
gender char(1),
level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar NOT NULL UNIQUE,
title varchar,
artist_id varchar,
year int,
duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar NOT NULL UNIQUE,
name varchar,
location varchar,
latitude float,
longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp NOT NULL,
hour int,
day int,
week int,
month int,
year int,
weekday int
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT into users (
VALUES (%s, %s, %s, %s, %s)
)
ON CONFLICT (user_id) 
DO UPDATE
    SET level  = users.level WHERE users.level = 'paid';
""")

song_table_insert = ("""
INSERT into songs (
VALUES (%s, %s, %s, %s, %s)
)
ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = ("""
INSERT into artists (
VALUES (%s, %s, %s, %s, %s)
)
ON CONFLICT (artist_id) 
DO NOTHING;
""")


time_table_insert = ("""
INSERT into time (
VALUES (%s, %s, %s, %s, %s, %s, %s)
)
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id
FROM songs
INNER JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title = %s
AND artists.name = %s
AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
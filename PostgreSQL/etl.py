import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json


def process_song_file(cur, filepath):
    """
    This function processes the data describing songs and artists.
    Inputs:
    - cur - cursor for psycopg2 connection to the database,
    - filepath - path of the directory containing the data for songs that needs processing
    
    Output:
    - inserting processed song data to the 'songs' dataset,
    - inserting processed artists data to the 'artists' dataset.
    
    TODO: change to Sphinx template, if only was possible
    
    """
    
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    # find proper columns
    song_data = pd.DataFrame(df[['song_id', 'title', 'artist_id', 'year', 'duration']])
    # change the type so that it fits the dataset
    song_data = song_data.T.astype({
        "song_id": str, 
        "title": str,
        "artist_id": str,
        "year": int,
        "duration": float
    })
    # change to list of lists
    song_data = [list(i) for i in (song_data.values)]
    cur.executemany(song_table_insert, song_data)
    
    # insert artist record
    artist_data = pd.DataFrame(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]).T.drop_duplicates()
    artist_data = [list(i) for i in (artist_data.values)]
    cur.executemany(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function processes the data describing logs of activity of customers.
    Inputs:
    - cur - cursor for psycopg2 connection to the database,
    - filepath - path of the directory containing the data for logs that needs processing.
    
    Output:
    - inserting processed data on time of activity to the 'time' dataset,
    - inserting processed users data to the 'users' dataset,
    - inserting processed songplays records.
    
    TODO: change to Sphinx template, if only was possible
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.DataFrame(pd.to_datetime(df.ts, unit = 'ms'))
    
    # insert time data records
    t['hour'] = t.ts.dt.hour
    t['day'] = t.ts.dt.day
    t['week'] = t.ts.dt.week
    t['month'] = t.ts.dt.month
    t['year'] = t.ts.dt.year
    t['weekday'] = t.ts.dt.weekday
    
    # insert time data records
    time_df = t

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.astype({
        "userId": int, 
        "firstName": str,
        "lastName": str,
        "gender": str,
        "level": str
    })
    user_df = user_df.drop_duplicates(
        ['userId','firstName','lastName','gender']
    )
    user_df = [list(i) for i in user_df.values]

    # insert user records
    cur.executemany(user_table_insert, user_df)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit = 'ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function (generator, kinda) loops over the directory 'filepath' to find all the files that need processing using function 'func'.
    Inputs:
    - cur - cursor for psycopg2 connection to the database,
    - conn - connection to that database,
    - filepath - path of the directory containing the data that needs processing,
    - func - function that should be used to process the data    
    
    TODO: change to Sphinx template, if only was possible
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
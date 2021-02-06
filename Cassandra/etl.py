# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster
from create_tables import *


# f - create a list of directories
def process_data(session, filepath, func):
    """
    This function (generator, kinda) loops over the directory 'filepath' to find all the files that need processing using function 'func'.
    Inputs:
    - session - session for connection to the database,
    - filepath - path of the directory containing the data that needs processing,
    - func - function that should be used to process the data    
    
    TODO: change to Sphinx template, if only was possible
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.csv'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(session, datafile)
        print('{}/{} files processed.'.format(i, num_files))

        
def process_csv_data(session, datafile):
    """
    """
    
    # extract
    my_file = pd.read_csv(datafile)
    my_file = my_file[['artist','firstName','gender','itemInSession','lastName',
                       'length','level','location','sessionId','song','userId']]
    my_file = my_file[~my_file.artist.isna()]
    # transform 
    # Table1
    query_1 = "INSERT INTO artists (artist, itemInSession, length, sessionId, song)"
    query_1 = query_1 + " VALUES (%s, %s, %s, %s, %s)"

    # Table2
    query_2 = "INSERT INTO artist_listened (artist, firstName, lastName, sessionId, song, userId, itemInSession)"
    query_2 = query_2 + " VALUES (%s, %s, %s, %s, %s, %s, %s)"

    # Table3
    query_3 = "INSERT INTO songs_listened (firstName, lastName, song)"
    query_3 = query_3 + " VALUES (%s, %s, %s)"
    
    # load
    for i, line in my_file.iterrows():
        session.execute(query_1, (line[0], int(line[3]), float(line[5]), int(line[8]), line[9]))
        session.execute(query_2, (line[0], line[1], line[4], int(line[8]), line[9], int(line[10]), int(line[3])))
        session.execute(query_3, (line[1], line[4], line[9]))
    

def main():
    
    print('Connecting sesssion...')
    cluster, session = connect_session()
    print('Creating keyspace...')
    session = create_keyspace(session)
    print('Keyspace created.')

    process_data(session, filepath='event_data', func=process_csv_data)
    
    #-------------------------------------------------------#
    # Queries
    # 1
    sessionId = 338
    itemInSession = 4
    query = f"select artist, song, length from artists WHERE sessionId={sessionId} AND itemInSession={itemInSession}"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print (row)
    
    #-------------------------------------------------------#
    # 2
    sessionId = 182
    userId = 10
    query = f"select artist, song, firstName, lastName from artist_listened WHERE sessionId={sessionId} AND userId={userId} ORDER BY itemInSession ASC;"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)

    #-------------------------------------------------------#
    # 3
    song_name = 'All Hands Against His Own'
    query = "select firstName, lastName from songs_listened WHERE song='All Hands Against His Own'"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)
        
    #-------------------------------------------------------#
    
    print('Closing connection and ending the script.')
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
# Import Python packages 
import pandas as pd
import cassandra
import re
import os
from cassandra.cluster import Cluster

# f - create a list of directories
# read the queries from another file?


# f - create a cluster and connection to the database
def connect_session():
    """
    """
    # try, except in case of error
    try:
        cluster = Cluster()
        # To establish connection and begin executing queries, need a session
        session = cluster.connect()

    except Exception as e:
        print(e)
    
    return cluster, session
        
def create_keyspace(session):
    """
    """    
    # Create a Keyspace 
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )

    except Exception as e:
        print(e)
    
    # Set KEYSPACE to the keyspace specified above
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)
        
    return session


def do_query(query, session):
    """
    Performs 'query' action (creating table, droping table, etc) on session
    """
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

        
def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database,
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    print('Connecting sesssion...')
    cluster, session = connect_session()
    print('Creating database...')
    session = create_keyspace(session)
    print('Database created.')
    
    drop_tables = [
        "drop table artists",
        "drop table artist_listened",
        "drop table songs_listened"
    ]
    
    create_tables = [
        "CREATE TABLE IF NOT EXISTS artists (artist text, itemInSession int, length float, sessionId int, song text, PRIMARY KEY (sessionId, itemInSession))",
        "CREATE TABLE IF NOT EXISTS artist_listened (artist text, firstName text, lastName text, sessionId int, song text, userId int, itemInSession int, PRIMARY KEY ((sessionId, userId), itemInSession))",
        "CREATE TABLE IF NOT EXISTS songs_listened (firstName text, lastName text, song text, PRIMARY KEY (song, firstName, lastName))"
    ]
    
    print('Droping tables...')
    for i in drop_tables:
        do_query(i, session)
    print('Tables dropped.')
    
    print('Creating tables...')
    for i in create_tables:
        do_query(i, session)
    print('Done.')

    print('Closing connection and ending the script.')
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
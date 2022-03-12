import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# to use replace function when tuning user_df
import numpy as np

# to resolve error "can't adapt type 'numpy.int64'" encountered at inserting record into song table
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# for time table
from datetime import datetime
            
# to resolve warning "A value is trying to be set on a copy of a slice from a DataFrame" at time table
pd.options.mode.chained_assignment = None
        
def process_song_file(cur, filepath):
    """
    This function fills the song and artist dimension tables using log data which contains song information. 
    The log file as a whole is converted into a dataframe. Then, empty dataframes are created for both tables 
    followed by copy and insertion operations.
        Parameters:
            cur (cursor)   : Database connection cursor
            filepath (str) : Path to log file containing song records
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # create an empty data frame for song data
    song_data_df = pd.DataFrame()
    
    # get appropriate columns from song file df and set it to song data df
    song_data_df[['song_id','title','artist_id', 'year', 'duration']] = df[['song_id','title','artist_id', 'year', 'duration']]
    
    # insert song record
    for i, row in song_data_df.iterrows():
        cur.execute(song_table_insert, list(row))
        
    # create an empty data frame for artist data
    artist_data_df = pd.DataFrame()
    
    # get appropriate columns from song file df and set it to artist data df
    artist_data_df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']] = df[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']]
        
    # insert artist record
    for i, row in artist_data_df.iterrows():
        cur.execute(artist_table_insert, list(row))
    
def process_log_file(cur, filepath):
    """
    This function fills time and user dimension tables as well as songplays fact table using log data containing song play information
    The log file as a whole is converted into a dataframe. Then, empty dataframes are created for each table. 
    Time and user tables are filled through simple copy and insertion operations whereas songplays table needed some
    extra work since the log file lacks 'songid' and 'artistid' information. This is resolved using a song select query
    on joined 'songs' and 'artists' tables.
        Parameters:
            cur   (cursor) : Database connection cursor
            filepath (str) : Path to log file containing song play records
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df.loc[df['page'] == 'NextSong']
    
    # create empty dataframes
    t = pd.DataFrame()
    time_df = pd.DataFrame()
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # prepare data and labels
    time_data = [df.ts.values, t.dt.hour.values, t.dt.day.values,
                 t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values,
                 t.dt.weekday.values]
    column_labels = ['start_time', 'hour', 'day',
                     'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    
    # create empty dataframe for users
    user_df = pd.DataFrame()
    
    # load user table
    user_df =  df[['userId','firstName','lastName', 'gender', 'level']]
    user_df = user_df.replace('', np.nan, regex=True) # convert empties to Nan's to prevent "invalid input syntax" error
    user_df = user_df[user_df['userId'].notna()] # remove rows with id 'NaN'

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    
    # clean
    df = df.replace(r'^\s*$', np.nan, regex=True) # replace blanks with nan
    df = df[df['userId'].notna()] # and remove them
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None  # should be checked before insert?
        
        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]

        try: 
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print (e)
            print (songplay_data)
        
def process_data(cur, conn, filepath, func):
    """
    Traverses all the JSON files given in directory
    Applies the given function as parameter to each file
        Parameters:
            cur       (cursor) : Database connection cursor object
            conn  (connection) : Database connection object
            filepath     (str) : Directory path to data log files
            func    (function) : Reference to function which will be applied
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
    """
    Entry point of the application. Connects to 'sparkify' database.
    Fills the fact and dimension tables of database using log files.
    Closes connection when finishes processing log files.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 2020

@author: gari.ciodaro.guerra

Orchestration of all the other scripts to do the ETL process
"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """ETL function to  process the song 
    json produced by the application.

    Parameters
    ----------
    cur : psycopg2.connect.cursor
        object to execute PostgreSQL command in a database session.
    filepath : string
        file location of the json to process.
    """
    # open song file
    df =pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]]
    song_data = song_data.values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =  df[["artist_id","artist_name","artist_location",
                    "artist_latitude","artist_longitude"]].values[0].tolist() 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ETL function to  process the log 
    json produced by the application.

    Parameters
    ----------
    cur : psycopg2.connect.cursor
        object to execute PostgreSQL command in a database session.
    filepath : string
        file location of the json to process.
    """
    df =pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t =  pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = [df.ts,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,
                    t.dt.weekday]
    column_labels = ["timestamp","hour", "day", "week", "month", "year",
                     "weekday"]
    dictionary_time={column_labels[each]:time_data[each] for each in 
                    range(0,len(time_data))}
    time_df = pd.DataFrame(dictionary_time,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]
    user_df = user_df.drop_duplicates()
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

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
        songplay_data = [row.ts, row.userId, row.level]
        songplay_data=songplay_data+[songid, artistid]
        songplay_data=songplay_data+[row.sessionId,row.location,row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Auxiliar constructor to produce an ETL process dictated by func
    input parameter.

    Parameters
    ----------
    cur : psycopg2.connect.cursor
        object to execute PostgreSQL command in a database session.
    conn : psycopg2.connect
        object to make the conection to the database.
    filepath : string
        root location of the set of json to process.
    func : function
        available choices are:
            -process_song_file()
            -process_log_file()
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
    """General ETL constructor.
    """
    login="host=127.0.0.1 dbname=sparkifydb user=student password=student"
    conn = psycopg2.connect(login)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
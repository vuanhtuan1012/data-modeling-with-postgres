# -*- coding: utf-8 -*-
# @Author: anh-tuan.vu
# @Date:   2020-12-09 21:49:31
# @Last Modified by:   anh-tuan.vu
# @Last Modified time: 2020-12-12 01:44:26

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Summary
    - Reads data from a song file in JSON format
    - Insert data to two table: songs and artists

    Args:
        cur (TYPE): cursor of database connection
        filepath (TYPE): path to song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[[
        'song_id', 'title', 'artist_id', 'year', 'duration'
    ]].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[[
        'artist_id', 'artist_name', 'artist_location',
        'artist_latitude', 'artist_longitude'
    ]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Summary
    - Read data from a log file in JSON format
    - Insert data to three tables: time, users, songplays

    Args:
        cur (TYPE): cursor of database connection
        filepath (TYPE): path to log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (
        t, t.dt.hour, t.dt.day, t.dt.isocalendar().week,
        t.dt.month, t.dt.year, t.dt.weekday
    )
    column_labels = (
        'timestamp', 'hour', 'day', 'weekofyear',
        'month', 'year', 'weekday'
    )
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    time_lst = [tuple(i) for i in time_df.to_numpy()]
    cur.executemany(time_table_insert, time_lst)

    # insert user records
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_lst = [tuple(i) for i in user_df.to_numpy()]
    cur.executemany(user_table_insert, user_lst)

    # insert songplay records
    songplay_data = list()
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data.append((
            pd.to_datetime(row.ts, unit='ms'), row.userId,
            row.level, songid, artistid, row.sessionId,
            row.location, row.userAgent
        ))
    cur.executemany(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Summary
    - Get absolute paths of all data files in JSON format
    - Call function to deal with these files

    Args:
        cur (TYPE): cursor of database connection
        conn (TYPE): connection to database
        filepath (TYPE): path to look for data files
        func (TYPE): function to deal with a data file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
        user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

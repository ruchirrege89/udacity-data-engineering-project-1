import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Brief Description:
    Open the Song file and retrieve the relevant columns to populate the songs and artists tables

    Arguments:
    cur - cursor with postgress database connection
    filepath - path of the file

    Return Types:
    Nothing

    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    #songcols = ['song_id','title','artist_id','year','duration']
    #song_data = df[songcols].values.flatten().tolist()
    # Taking the suggested approach in the review as it is concise.
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_col = ['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artist_data=df[artist_col].values.flatten().tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Brief Description:
    Open the Logs file and retrieve the relevant columns to populate the users, time and songsplay tables

    Arguments:
    cur - cursor with postgress database connection
    filepath - path of the file

    Return Types:
    Nothing

    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    t.drop_duplicates(inplace=True)
    t.dropna(inplace=True)

    # insert time data records
    time_df = pd.DataFrame(index=t.index)
    time_df['start_time'] = t
    time_df['hour'] = t.dt.hour
    time_df['day'] = t.dt.day
    time_df['week'] = t.dt.week
    time_df['month'] = t.dt.month
    time_df['year'] = t.dt.year
    time_df['weekday'] = t.dt.weekday

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

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
        start_time = pd.Timestamp(row['ts'], unit='ms')
        songplay_data = (start_time, row['userId'], row['level'],songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)





def process_data(cur, conn, filepath, func):
    """
    Brief Description:
    Gets the file list from the file path and calls the song and log process functions for each file.

    Arguments:
    conn - connection to the postgress database
    cur - cursor with postgress database connection
    filepath - path of the file
    func - names of the functions (song and log file processing)

    Return Types:
    Nothing

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
    Brief Description:
    Staring point of the ETL pipeline

    Arguments:
    Nothing

    Return Types:
    Nothing

    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

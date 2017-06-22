# Copyright 2017. All rights reserved. See AUTHORS.txt
# Licensed under the Apache License, Version 2.0 which is in LICENSE.txt
"""
Utilities to scrape data from URLs and databases.

File download function adapted from
http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py

"""

import datetime
import getpass
import hashlib
import os, sys
import psycopg2
import requests
import zipfile
import pandas as pd

download_metadata_fields = ('filename', 'url', 'download_timestamp_utc', 'sha1')
# A standard size for chunking data for disk writes: 64kb = 2^16 = 65536
BLOCKSIZE = 65536

def download_file(url, local_path):
    """
    Robustly download the contents of a url to a local file.
    Return metadata suitable for a log file:
        (local_path, url, timestamp, sha1_hash)
    See also: download_metadata_fields
    """
    r = requests.get(url, stream=True)
    hasher = hashlib.sha1()
    with open(local_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=BLOCKSIZE): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                hasher.update(chunk)
    timestamp = datetime.datetime.utcnow()
    return (local_path, url, timestamp, hasher.hexdigest())


def unzip(file_list):
    for path in file_list:
        unzip_name = os.path.splitext(path)[0]
        # Skip unzipping if the directory exists
        if not os.path.isdir(unzip_name):
            print "Unzipping " + path
            zip_ref = zipfile.ZipFile(path, 'r')
            zip_ref.extractall(unzip_name)
            zip_ref.close()
        else:
            print "Skipping "+unzip_name+" because it was already unzipped."


def connect_to_db_and_push_df(df, col_formats, table, database='postgres', host='localhost', port=5433, user=None, password=None):
    if user == None:
        user = getpass.getpass('Enter username for database {}:'.format(database))
    if password == None:    
        password = getpass.getpass('Enter database password for user {}:'.format(user))
    try:
        con = psycopg2.connect(database=database, user=user, host=host,
            port=port, password=password)
        print "Connection to database established..."
    except:
        sys.exit("Error connecting to database {} at host {}:{}.".format(database,host,port))

    cur = con.cursor()
    try:
        args_str = ','.join(cur.mogrify(col_formats, x[1]) for x in df.iterrows())
        query = "INSERT INTO "+table+" VALUES " + args_str+";"
        cur.execute(query)
        print "Successfully pushed values"
    except Exception, e:
        print 'Query execution failed with error: {}'.format(e)
        return None
    con.commit()
    cur.close()
    con.close()
    print 'Database connection closed.'
    return


def connect_to_db_and_run_query(query, database='postgres', host='localhost', port=5433, user=None, password=None, quiet=False):
    if user == None:
        user = getpass.getpass('Enter username for database {}:'.format(database))
    if password == None:
        password = getpass.getpass('Enter database password for user {}:'.format(user))
    try:
        con = psycopg2.connect(database=database, user=user, host=host,
            port=port, password=password)
        if not quiet:
            print "Connection to database established..."
    except:
        sys.exit("Error connecting to database {} at host {}:{}.".format(database,host,port))

    cur = con.cursor()
    try:
        cur.execute(query)
        # fetchall() returns a list of tuples with the rows resulting from the query
        # column names must be gotten from the cursor's description
        if cur.description != None:
            df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
            if not quiet:
                print 'Successfully executed query: returning results.'
            return df
        else:
            if not quiet:
                print 'Successfully executed query with no results.'
    except Exception, e:
        print 'Query execution failed with error: {}'.format(e)
        return None
    con.commit()
    cur.close()
    con.close()
    if not quiet:
        print 'Database connection closed.'
    return


def append_historic_output_to_csv(fpath, df):
        write_header = not os.path.isfile(fpath)
        with open(fpath, 'ab') as outfile:
            df.to_csv(outfile, sep='\t', header=write_header, encoding='utf-8', index=False)
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
import os
import psycopg2
import requests
import zipfile

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


def connect_to_db_and_run_query(query, database='postgres', host='localhost', port=5433):
    user = getpass.getpass('Enter username for database {}:'.format(database))
    password = getpass.getpass('Enter database password for user {}:'.format(user))
    try:
        con = psycopg2.connect(database=database, user=user, host=host,
            port=port, password=password)
        print "Connection to database established..."
    except:
        sys.exit("Error connecting to database {} at host {}:{}.".format(database,host,port))

    cur = con.cursor()
    try:
        cur.execute(query)
        # fetchall() returns a list of tuples with the rows resulting from the query
        output = cur.fetchall()
        print 'Successfully executed query: returning results.'
        return output
    except:
        print 'Query execution failed.'
        return None
    cur.close()
    con.close()
    print 'Database connection closed.'
    
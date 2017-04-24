# Copyright 2017. All rights reserved. See AUTHORS.txt
# Licensed under the Apache License, Version 2.0 which is in LICENSE.txt

# Utility for robustly downloading a file and returning metadata
# Adapted from http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py


import datetime
import requests
import hashlib
import zipfile
import os

download_metadata_fields = ('filename', 'url', 'download_timestamp_utc', 'sha1')
# A standard size for chunking data for disk writes: 64kb = 2^16 = 65536
BLOCKSIZE = 65536

def download_file(url, local_path):
    """
    Download the contents of a url to a local file, and plays nice with
    large files. Return metadata suitable for a log file:
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

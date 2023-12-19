import requests
import time
import logging
from datetime import timedelta
from google.cloud import storage
from google.cloud.storage.blob import Blob
import redis
import os
import threading
import sys


def upload_data_to_gcs(filename):
    client = storage.Client().from_service_account_json('nikhil.json')
    bucket_name = 'mapreduce02'
    input_prefix = 'input/'
    bucket = client.bucket(bucket_name)
    local_path = filename
    blob = Blob(input_prefix + os.path.basename(local_path), bucket)
    blob.upload_from_filename(local_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    start_time = time.time()
    try:
        if len(sys.argv) < 2:
            print("Please provide a filename.")
            sys.exit(1)
        filename = sys.argv[1].split("=")[1]
        print(f"Uploading file: {filename}")
        upload_data_to_gcs(filename)
    except Exception as e:
        logging.error(f"Error in uploading data to GCS: {e}")
    finally:
        print("Data uploaded to GCS.")
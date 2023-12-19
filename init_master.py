import requests
import time
import logging
from datetime import timedelta
from google.cloud import storage
from google.cloud.storage.blob import Blob
import redis
import os
import threading

def upload_data_to_gcs():
    client = storage.Client().from_service_account_json('nikhil.json')
    bucket_name = 'mapreduce02'
    input_prefix = 'input/'
    bucket = client.bucket(bucket_name)
    input_files = []
    local_directory = 'input/'  # Specify the local directory path
    for filename in os.listdir(local_directory):
        if os.path.isfile(os.path.join(local_directory, filename)):
            input_files.append(filename)
    for input_file in input_files:
        local_path = os.path.join(local_directory, input_file)
        blob = Blob(input_prefix + input_file, bucket)
        blob.upload_from_filename(local_path)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    start_time = time.time()
    master_url = "https://us-central1-nikhil-srirangam-fall2023.cloudfunctions.net/master_main"
    try:
        print("Started uploading data to GCS...")
        upload_thread = threading.Thread(target=upload_data_to_gcs)
        upload_thread.start()
        upload_thread.join()
    except Exception as e:
        logging.error(f"Error in uploading data to GCS: {e}")
    finally:
        print("Data uploaded to GCS.")
    try:
        print("Started Connecting to Redis...")
        r = redis.Redis(
                host='redis-15947.c238.us-central1-2.gce.cloud.redislabs.com', port=15947,
                password="otA2f9Fe4sm8z7TpdCk2u0DS2looQCaV", # use your Redis password
                )
    except Exception as e:
        logging.error(f"Error connecting to Redis: {e}")
    finally:
        print("Connected to Redis.")
    try:
        result = r.flushdb()
        r.set('map_reduce_manual_indexing_done', 'NO')
        r.set('map_reduce_processing', 'NO')
    except Exception as e:
        logging.error(f"Error flushing Redis database: {e}")
    try:
        print("Starting Master process...")
        response = requests.post(master_url)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Error in  process: {e}")
    finally:
        print(f"Master process finished with status code:", response.status_code)
    
    end_time = time.time()
    logging.info("Ending master process at %s total time taken ", time.time())
    duration = timedelta(seconds=end_time - start_time)
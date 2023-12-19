import functions_framework
import json
import logging
import multiprocessing
import os
import shutil
import time
from collections import defaultdict
from hashlib import sha256
from multiprocessing import Event, Process
from datetime import timedelta
import requests
import redis

from google.cloud import storage
from google.cloud.storage.blob import Blob
import re
import functions_framework
client = storage.Client.from_service_account_json('nikhil.json')
bucket_name = 'mapreduce02' 
bucket = client.bucket(bucket_name)

def call_mapper_function(file_paths, mapper_index, num_reducers, result_queue):
    mapper_url = "https://us-central1-nikhil-srirangam-fall2023.cloudfunctions.net/mapper"
    data = {
        "file_paths": file_paths,
        "mapper_index": mapper_index,
        "num_reducers": num_reducers
    }
    try:
        response = requests.post(mapper_url, json=data)
        response.raise_for_status()
        result_queue.put((mapper_index, response.status_code))
    except requests.RequestException as e:
        logging.error(f"Error in Mapper process {mapper_index}: {e}")
        result_queue.put((mapper_index, None))

def call_reducer_function(reducer_index, num_mappers, result_queue):
    mapper_url = "https://us-central1-nikhil-srirangam-fall2023.cloudfunctions.net/reducer1"
    data = {
        "reducer_index": reducer_index,
        "num_mappers": num_mappers
    }
    try:
        response = requests.post(mapper_url, json=data)
        response.raise_for_status()
        result_queue.put((reducer_index, response.status_code))
    except requests.RequestException as e:
        logging.error(f"Error in Reducer process {reducer_index}: {e}")
        result_queue.put((reducer_index, None))

def remove_files_in_bucket_directory(bucket, directory_name):
    blobs = bucket.list_blobs(prefix=directory_name)
    for blob in blobs:
        blob.delete()

def master(file_path):
    
    try:
        file_name = os.path.basename(file_path)
        os.makedirs('mapper_outputs', exist_ok=True)
        os.makedirs('reducer_outputs', exist_ok=True)
        with open('config.json', 'r') as config_file:
            config_data = json.load(config_file)
            num_reducers = config_data['reducers']
            num_mappers = config_data['mappers']
            password = config_data['password']
        # num_mappers = 1
        # num_reducers = 5
        input_files = []
        client = storage.Client().from_service_account_json('nikhil.json')
        bucket_name = 'mapreduce02'
        input_prefix = 'input/'
        bucket = client.bucket(bucket_name)
        r = redis.Redis(host='redis-15947.c238.us-central1-2.gce.cloud.redislabs.com', port=15947,
        # username="default", # use your Redis user. More info https://redis.io/docs/management/security/acl/
        password=password,
        # ssl=False,
        )
        r.set('map_reduce_processing', 'YES')
        print("Starting mappers...")
        # mapper_file_paths = [[] for _ in range(num_mappers)]
        mapper_file_paths = [[file_name]]
        mapper_processes = []
        result_queue = multiprocessing.Queue()
        mapper_process = multiprocessing.Process(target=call_mapper_function, args=(mapper_file_paths[0], 0, num_reducers, result_queue))
        mapper_process.start()
        mapper_processes.append(mapper_process)
        mapper_process.join()
        if not result_queue.empty():
          mapper_index, status = result_queue.get()
          if status == 200:
              logging.info(f"Mapper completed successfully.")
          else:
              logging.error(f"Mapper encountered an error or did not return a status.")

        print("All mappers finished.")
        reducer_processes = []
        reducer_queue = multiprocessing.Queue()
        print("Starting reducers...")
        for i in range(num_reducers):
            reducer_process = multiprocessing.Process(target=call_reducer_function, args=(i, num_mappers,reducer_queue))
            reducer_process.start()
            reducer_processes.append(reducer_process)
        for reducer in reducer_processes:
            reducer.join()
        while not reducer_queue.empty():
            reducer_index, status = reducer_queue.get()
            if status == 200:
                logging.info(f"Reducer {reducer_index} completed successfully.")
            else:
                logging.error(f"Reducer {reducer_index} encountered an error or did not return a status.")
        # Clear reducer_outputs directory
        reducer_output_dir = 'reducer_outputs'
        reducer_input_dir = 'reducer_inputs'
        mapper_output_dir = 'mapper_outputs'
        # remove_files_in_bucket_directory(bucket, mapper_output_dir)
        # remove_files_in_bucket_directory(bucket, reducer_output_dir)
        return 'Processing complete', 200
    except Exception as e:
        logging.error(f"Master process encountered an error: {e}")
        return 'Processing failed', 400
    finally:
        r.set('map_reduce_processing', 'NO')
        logging.info("Master process finished.")


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]
    if name.startswith("input/"):
        try:
            r = redis.Redis(host='redis-15947.c238.us-central1-2.gce.cloud.redislabs.com', port=15947, password='otA2f9Fe4sm8z7TpdCk2u0DS2looQCaV')
            word = 'map_reduce_manual_indexing_done'
            data = r.get(word)
            data = data.decode('utf-8')  # Decode byte string to string
            if data == 'YES':
                result_message, status_code = master(name)
                print(result_message)
            else:
                print(f"The function will not proceed as the manual indexing is not done yet.")
        except Exception as e:
            print(f"Error connecting to Redis: {e}")

    else:
      print(f"Change detected outside 'input/' folder: {name}")


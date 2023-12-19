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

from mapper import Mapper
from reducer import Reducer

class MasterProcess(Process):
    def __init__(self):
        super().__init__()


    def run(self):
        try:
        # Configuring logging
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
            start_time = time.time()
            logging.info("Starting master process at %s", time.time())
            # checking for mapper and reducer directories
            os.makedirs('mapper_outputs', exist_ok=True)
            os.makedirs('reducer_outputs', exist_ok=True)

            # reading config.json file
            with open('config.json', 'r') as config_file:
                config_data = json.load(config_file)
                num_reducers = config_data['reducers']
                num_mappers = config_data['mappers']
                password  = config_data['password']
                input_files = []
            
            client = storage.Client().from_service_account_json('nikhil.json')
            bucket_name = 'mapreduce02'
            input_prefix = 'input/'

            bucket = client.bucket(bucket_name)

            for blob in bucket.list_blobs(prefix=input_prefix):
                if not blob.name.endswith('/'):  # Check if the blob is not a directory                
                    input_files.append(os.path.basename(blob.name))

            
                # Read files from input_dir and append them to input_files
                
            r = redis.Redis(host='redis-15947.c238.us-central1-2.gce.cloud.redislabs.com', port=15947,
            # username="default", # use your Redis user. More info https://redis.io/docs/management/security/acl/
            password=password
            # password="otA2f9Fe4sm8z7TpdCk2u0DS2looQCaV", # use your Redis password
            # ssl=False,
            )
            completion_events = []
            print("Starting mappers...")
            # array to store file path for each mapper
            mapper_file_paths = [[] for _ in range(num_mappers)]
            for i, file_path in enumerate(input_files):
                mapper_index = i % num_mappers
                mapper_file_paths[mapper_index].append(file_path)
            mapper_processes = []
            result_queue = multiprocessing.Queue()
            for i in range(num_mappers):
                mapper_process = multiprocessing.Process(target=self.call_mapper_function, args=(mapper_file_paths[i], i, num_reducers,result_queue))
                mapper_process.start()
                mapper_processes.append(mapper_process)

            for process in mapper_processes:
                process.join()
            while not result_queue.empty():
                mapper_index, status = result_queue.get()
                if status == 200:
                    logging.info(f"Mapper {mapper_index} completed successfully.")
                else:
                    logging.error(f"Mapper {mapper_index} encountered an error or did not return a status.")

            print("All mappers finished.")
            reducer_processes = []
            reducer_queue = multiprocessing.Queue()
            print("Starting reducers...")
            for i in range(num_reducers):
                reducer_process = multiprocessing.Process(target=self.call_reducer_function, args=(i, num_mappers,reducer_queue))
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
            # self.remove_files_in_local_dir(reducer_input_dir)
            # self.remove_files_in_local_dir(reducer_output_dir)
            # self.remove_files_in_local_dir(mapper_output_dir)
            # self.remove_files_in_local_dir('red_outputs')
            #cleanup gcs buckets
            self.remove_files_in_bucket_directory(bucket, mapper_output_dir)
            self.remove_files_in_bucket_directory(bucket, reducer_output_dir)

            print("Mapper and reducer outputs cleared.")
            end_time = time.time()
            logging.info("Ending master process at %s total time taken ", time.time())
            duration = timedelta(seconds=end_time - start_time)

            # Format the duration as HH:MM:SS
            formatted_duration = str(duration)

            logging.info("Total time taken: %s", formatted_duration)
        except Exception as e:
            logging.error(f"Master process encountered an error: {e}")
        finally:
            r.set("map_reduce_manual_indexing_done", "YES")
            logging.info("Master process finished.")



    def remove_files_in_bucket_directory(self,bucket, directory_name):
        blobs = bucket.list_blobs(prefix=directory_name)
        for blob in blobs:
            blob.delete()

            
    def call_mapper_function(self,file_paths, mapper_index, num_reducers, result_queue):
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

    def call_reducer_function(self, reducer_index, num_mappers, result_queue):
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

    def remove_files_in_local_dir(self,output_path):
        for filename in os.listdir(output_path):
            file_path = os.path.join(output_path, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logging.error(f"Error clearing reducer output file/directory {file_path}: {e}")

        


if __name__ == "__main__":
    # print(requests.__version__)
    map_reduce_process = MasterProcess()
    map_reduce_process.start()
    map_reduce_process.join()

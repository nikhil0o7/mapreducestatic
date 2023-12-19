import json
import logging
import os
from collections import defaultdict
from hashlib import sha256
from multiprocessing import Event, Process
import json
import logging
import os
from collections import defaultdict
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import redis

class Reducer(Process):
    def __init__(self, reducer_index, completion_event, num_mappers):
        Process.__init__(self)
        self.reducer_index = reducer_index
        self.completion_event = completion_event
        self.num_mappers = num_mappers

    def run(self):
        logging.basicConfig(level=logging.INFO)
        reduced_data = defaultdict(lambda: {'count': 0, 'filenames': set()})
        client = storage.Client().from_service_account_json('nikhil.json')
        bucket_name = 'mapreduce02'
        input_prefix = 'mapper_outputs/'
        local_file_path = 'reducer_inputs'
        files_key = f'r{self.reducer_index}.json'
        os.makedirs(local_file_path, exist_ok=True)

        bucket = client.bucket(bucket_name)
        download_tasks = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Filtering and submitting download tasks
            for blob in bucket.list_blobs(prefix=input_prefix):
                if blob.name.endswith(files_key) and not blob.name.endswith('/'):
                    task =executor.submit(self.download_blob, blob, input_prefix, local_file_path)
                    download_tasks.append(task)

            for task in as_completed(download_tasks):
                try:
                    result = task.result()
                    if result:
                        logging.info(f"Completed download: {result}")
                except Exception as e:
                    logging.error(f"Error downloading file: {e}")


        for mapper_index in range(self.num_mappers):
            intermediate_file_name = f'reducer_inputs/m{mapper_index}r{self.reducer_index}.json'
            try:
                with open(intermediate_file_name, 'r') as file:
                    for line in file:
                        word, filename, count = json.loads(line)
                        reduced_data[word]['count'] += count
                        reduced_data[word]['filenames'].add(filename)
            except FileNotFoundError:
                logging.warning(f"Reducer {self.reducer_index} did not find file {intermediate_file_name}, skipping.")
                continue
            except Exception as e:
                logging.error(f"Reducer {self.reducer_index} encountered an error: {e}")
                continue

        # Convert set to list before writing to JSON
        reduced_data = {word: {'count': data['count'], 'filenames': list(data['filenames'])} for word, data in reduced_data.items()}

        # Writing the reduced data to the final output file
        output_dir = f'reducer_outputs/'
        os.makedirs(output_dir, exist_ok=True)
        try:
            r = redis.Redis(
                host='redis-15947.c238.us-central1-2.gce.cloud.redislabs.com', port=15947,
                username="default", # use your Redis user. More info https://redis.io/docs/management/security/acl/
                password="otA2f9Fe4sm8z7TpdCk2u0DS2looQCaV", # use your Redis password
                # ssl=False,
                )
        except Exception as e:
            logging.error(f"Error connecting to Redis: {e}")
        output_file_path = os.path.join(output_dir, f'reducer_output_{self.reducer_index}.json')
        try:
            with open(output_file_path, 'w') as file:
                json.dump(reduced_data, file, indent=4)
                logging.info(f"Reducer {self.reducer_index} finished writing output.")
            self.upload_file_to_gcs(bucket, output_file_path)

        except Exception as e:
            logging.error(f"Reducer {self.reducer_index} failed to write output: {e}")

        try:
            logging.info(f"Reducer {self.reducer_index} started processing reducer output to redis.")
            self.process_reducer_output(r,output_file_path)
        except Exception as e:
            logging.error(f"Error processing reducer output: {e}")
        finally:
            logging.info(f"Reducer {self.reducer_index} finished processing reducer output to redis.")
            self.completion_event.set()

        # finally:
        #     logging.info(f"Reducer {self.reducer_index} finished uploading file.")
        #     self.completion_event.set()

    def process_reducer_output(self,r,output_file_path):
        try:
            with open(output_file_path, 'r') as file:
                reducer_data = json.load(file)
                if isinstance(reducer_data, dict):
                    for word, data in reducer_data.items():
                        count = data['count']
                        filenames = data['filenames']
                        # Check if the word already exists in Redis
                        if r.hexists('words', word):
                            existing_data = json.loads(r.hget('words', word))
                            existing_data['count'] += count
                            existing_data['filenames'] = list(set(existing_data['filenames'] + filenames))
                            r.hset('words', word, json.dumps(existing_data))
                            logging.info(f"Updated data for word '{word}' in Redis.")
                        else:
                            r.hset('words', word, json.dumps({'count': count, 'filenames': filenames}))
                            logging.info(f"Stored data for word '{word}' in Redis.")
                else:
                    logging.error(f"Invalid reducer output file format: {output_file_path}")
        except Exception as e:
            logging.error(f"Error reading reducer output file {output_file_path}: {e}")


    def upload_file_to_gcs(self, bucket, file_path, timeout=120, max_retries=3):
        for attempt in range(max_retries):
            try:
                blob = bucket.blob(file_path)
                # chunk_size for chunked upload
                blob.chunk_size = 5 * 1024 * 1024  
                blob.upload_from_filename(file_path, timeout=timeout)
                print(f"Reducer{self.reducer_index} Uploaded {file_path}")
                break
            except Exception as e:
                print(f"Error uploading {file_path}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff

    def download_blob(self,blob, input_prefix, local_file_path):
        relative_path = blob.name[len(input_prefix):]
        full_local_file_path = os.path.join(local_file_path, relative_path)
        os.makedirs(os.path.dirname(full_local_file_path), exist_ok=True)
        try:
            blob.download_to_filename(full_local_file_path)
            logging.info(f"Downloaded {blob.name} to {full_local_file_path}")
        except Exception as e:
            logging.error(f"Error downloading file {blob.name}: {e}")

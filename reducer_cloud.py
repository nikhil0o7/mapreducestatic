from collections import defaultdict
import os
import logging
import re
import json
from google.cloud import storage
import functions_framework
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from multiprocessing import Process, Event
import redis


# Initialize Cloud Storage client
client = storage.Client.from_service_account_json('nikhil.json')
bucket_name = 'mapreduce02' 
bucket = client.bucket(bucket_name)

def remove_trash(text):
    clean = re.sub(r'[^\w\s]', ' ', text)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

def fnv1a_hash(input_string):
    # """32-bit FNV-1a hash function"""
    FNV_prime = 0x01000193
    hash = 0x811C9DC5

    for byte in input_string.encode('utf-8'):
        hash ^= byte
        hash = (hash * FNV_prime) & 0xffffffff
    return hash

def download_blob(blob, input_prefix, local_file_path):
    relative_path = blob.name[len(input_prefix):]
    full_local_file_path = os.path.join(local_file_path, relative_path)
    os.makedirs(os.path.dirname(full_local_file_path), exist_ok=True)
    try:
        blob.download_to_filename(full_local_file_path)
        logging.info(f"Downloaded {blob.name} to {full_local_file_path}")
    except Exception as e:
        logging.error(f"Error downloading file {blob.name}: {e}")


def upload_file_to_gcs(bucket, file_path, timeout=120, max_retries=3):
    for attempt in range(max_retries):
        try:
            blob = bucket.blob(file_path)
            # chunk_size for chunked upload
            blob.chunk_size = 2 * 1024 * 1024  
            blob.upload_from_filename(file_path, timeout=timeout)
            logging.info(f"Uploaded {file_path}")
            break
        except Exception as e:
            logging.error(f"Error uploading {file_path}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                
# def process_reducer_output(r,output_file_path):
#     try:
#         with open(output_file_path, 'r') as file:
#             reducer_data = json.load(file)
#             if isinstance(reducer_data, dict):
#                 for word, data in reducer_data.items():
#                     count = data['count']
#                     filenames = data['filenames']
#                     # Check if the word already exists in Redis
#                     if r.hexists('words', word):
#                         existing_data = json.loads(r.hget('words', word))
#                         existing_data['count'] += count
#                         existing_data['filenames'] = list(set(existing_data['filenames'] + filenames))
#                         r.hset('words', word, json.dumps(existing_data))
#                         logging.info(f"Updated data for word '{word}' in Redis.")
#                     else:
#                         r.hset('words', word, json.dumps({'count': count, 'filenames': filenames}))
#                         logging.info(f"Stored data for word '{word}' in Redis.")
#             else:
#                 logging.error(f"Invalid reducer output file format: {output_file_path}")
#     except Exception as e:
#         logging.error(f"Error reading reducer output file {output_file_path}: {e}")
def process_reducer_output(r, output_file_path):
    try:
        with open(output_file_path, 'r') as file:
            reducer_data = json.load(file)

            if isinstance(reducer_data, dict):
                for word, data in reducer_data.items():
                    # New structure has filenames as keys and counts as values
                    filenames_counts = data['filenames']

                    # Check if the word already exists in Redis
                    if r.hexists('words', word):
                        existing_data = json.loads(r.hget('words', word))
                        existing_filenames_counts = existing_data.get('filenames', {})

                        # Merge counts per filename
                        for filename, count in filenames_counts.items():
                            existing_filenames_counts[filename] = existing_filenames_counts.get(filename, 0) + count

                        # Update the existing data with the new counts
                        existing_data['filenames'] = existing_filenames_counts
                        r.hset('words', word, json.dumps(existing_data))
                        logging.info(f"Updated data for word '{word}' in Redis.")
                    else:
                        # If the word doesn't exist in Redis, add it directly
                        r.hset('words', word, json.dumps({'filenames': filenames_counts}))
                        logging.info(f"Stored data for word '{word}' in Redis.")
            else:
                logging.error(f"Invalid reducer output file format: {output_file_path}")

    except Exception as e:
        logging.error(f"Error reading reducer output file {output_file_path}: {e}")


@functions_framework.http
def reducer_function(request):
    try:
        reduced_data = defaultdict(lambda: {'filenames': {}})
        request_json = request.get_json(silent=True)

        reducer_index = request_json['reducer_index']
        num_mappers = request_json['num_mappers']
        logging.basicConfig(level=logging.INFO)
        words = {}
        input_prefix = 'mapper_outputs/'
        local_file_path = 'reducer_inputs'
        files_key = f'r{reducer_index}.json'
        download_tasks = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Filtering and submitting download tasks
            for blob in bucket.list_blobs(prefix=input_prefix):
                if blob.name.endswith(files_key) and not blob.name.endswith('/'):
                    task =executor.submit(download_blob, blob, input_prefix, local_file_path)
                    download_tasks.append(task)

            for task in as_completed(download_tasks):
                try:
                    result = task.result()
                    if result:
                        logging.info(f"Completed download: {result}")
                except Exception as e:
                    logging.error(f"Error downloading file: {e}")

        for mapper_index in range(num_mappers):
            intermediate_file_name = f'reducer_inputs/m{mapper_index}r{reducer_index}.json'
            try:
                with open(intermediate_file_name, 'r') as file:
                    for line in file:
                        word, filename, count = json.loads(line)
                        reduced_data[word]['count'] += count
                        word, filename, count = json.loads(line)
                        if word not in reduced_data:
                            reduced_data[word] = {'filenames': {}}
                
                # Initialize the count for the filename if it doesn't exist for this word
                        if filename not in reduced_data[word]['filenames']:
                            reduced_data[word]['filenames'][filename] = 0

                # Add the count for this specific filename
                        reduced_data[word]['filenames'][filename] += count
            except FileNotFoundError:
                logging.warning(f"Reducer {reducer_index} did not find file {intermediate_file_name}, skipping.")
                continue
            except Exception as e:
                logging.error(f"Reducer {reducer_index} encountered an error: {e}")
                continue
        # Convert set to list before writing to JSON
        reduced_data = {word: {'count': data['count'], 'filenames': list(data['filenames'])} for word, data in reduced_data.items()}
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
        output_file_path = os.path.join(output_dir, f'reducer_output_{reducer_index}.json')
        try:
            with open(output_file_path, 'w') as file:
                json.dump(reduced_data, file, indent=4)
                logging.info(f"Reducer {reducer_index} finished writing output.")
            upload_file_to_gcs(bucket, output_file_path)
        except Exception as e:
            logging.error(f"Reducer {reducer_index} failed to write output: {e}")
        try:
            logging.info(f"Reducer {reducer_index} started processing reducer output to redis.")
            # process_reducer_output(r,output_file_path)
        except Exception as e:
            logging.error(f"Error processing reducer output: {e}")
        finally:
            logging.info(f"Reducer {reducer_index} finished processing reducer output to redis.")

        print(f"Reducer{reducer_index} finished it's process")
        return 'Processing complete', 200 
        


    except Exception as e:
        logging.error(f"Reducer {reducer_index} failed to write output: {e}")
        return f"Error: {e}", 500


        
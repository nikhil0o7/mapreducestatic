# import functions_framework

# @functions_framework.http
# def hello_http(request):
#     """HTTP Cloud Function.
#     Args:
#         request (flask.Request): The request object.
#         <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
#     Returns:
#         The response text, or any set of values that can be turned into a
#         Response object using `make_response`
#         <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
#     """
#     request_json = request.get_json(silent=True)
#     request_args = request.args

#     if request_json and 'name' in request_json:
#         name = request_json['name']
#     elif request_args and 'name' in request_args:
#         name = request_args['name']
#     else:
#         name = 'World'
#     return 'Hello {}!'.format(name)

import os
import logging
import re
import json
from google.cloud import storage
import functions_framework
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from multiprocessing import Process, Event



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

@functions_framework.http
def mapper_function(request):
    try:
        request_json = request.get_json(silent=True)
        if not request_json or 'file_paths' not in request_json:
            return 'No file paths provided', 400

        file_paths = request_json['file_paths']
        mapper_index = request_json['mapper_index']
        num_reducers = request_json['num_reducers']
        words = {}
        input_prefix = 'input/'
        local_file_path = 'input'
        os.makedirs(local_file_path, exist_ok=True)
        for blob in bucket.list_blobs(prefix=input_prefix):
            relative_path = blob.name[len(input_prefix):]
            if relative_path in file_paths:
                if not blob.name.endswith('/'):
                    full_local_file_path = os.path.join(local_file_path, relative_path)
                    os.makedirs(os.path.dirname(full_local_file_path), exist_ok=True)
                    try:
                        blob.download_to_filename(full_local_file_path)
                    except Exception as e:
                        logging.error(f"Error downloading file: {e}")
        for file_path in file_paths:
            full_path = os.path.join('input', file_path)
            if not os.path.exists(full_path):
                logging.error(f"File does not exist: {file_path}")
                continue
            with open(full_path, 'r') as file:
                input_text = file.read()
            clean_text = remove_trash(input_text)
            file_name = os.path.basename(file_path)
            for word in clean_text.split():
                if word not in words:
                    words[word] = []
                words[word].append((word, file_name, 1))

        gcs_files = set()
        mapper_output_dir = f'mapper_{mapper_index}'
        os.makedirs(mapper_output_dir, exist_ok=True)
        for word, file_counts in words.items():
            reducer_index = fnv1a_hash(word) % num_reducers
            intermediate_file_name = f'm{mapper_index}r{reducer_index}.json'
            intermediate_file_path = os.path.join(mapper_output_dir, intermediate_file_name)
            gcs_files.add(intermediate_file_path)
            with open(intermediate_file_path, 'a') as file:
                for word, file_name, count in file_counts:  
                    file.write(json.dumps((word, file_name, count)) + '\n')
        print(f"Mapper{mapper_index} started uploading files to gcs")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = {executor.submit(upload_file_to_gcs, bucket, file_path): file_path for file_path in gcs_files}
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    print(f"Successfully uploaded {file_path}")
                    future.result()
                except Exception as e:
                    print(f"Error with file {file_path}: {e}")
                

            print(f"Mapper{mapper_index} finished uploading files to gcs")
            return 'Processing complete', 200

    except Exception as e:
        logging.error(f"Mapper encountered an error: {e}")
        return f"Error: {e}", 500
    # finally:
    #     return 'Processing complete', 200
    # return 'Processing failed', 400




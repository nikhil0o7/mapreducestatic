from multiprocessing import Process, Event
import os.path
import re
import json
import os
import logging
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
# https://us-central1-nikhil-srirangam-fall2023.cloudfunctions.net/mapper
class Mapper(Process):
    def __init__(self, file_paths, mapper_index, num_reducers, completion_event):
        Process.__init__(self)
        self.file_paths = file_paths
        self.mapper_index = mapper_index
        self.completion_event = completion_event
        self.num_reducers = num_reducers

    def remove_trash(self, text):
        clean = re.sub(r'[^\w\s]', ' ', text)
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean

    def run(self):
        try:
            logging.basicConfig(level=logging.INFO)
            words = {}
            client = storage.Client().from_service_account_json('nikhil.json')
            bucket_name = 'mapreduce02'
            input_prefix = 'input/'

            bucket = client.bucket(bucket_name)
            local_file_path = 'input'
            os.makedirs(local_file_path, exist_ok=True)
            

            for blob in bucket.list_blobs(prefix=input_prefix):
                relative_path = blob.name[len(input_prefix):]
                if relative_path in self.file_paths:
                    if not blob.name.endswith('/'):
                        full_local_file_path = os.path.join(local_file_path, relative_path)
                        os.makedirs(os.path.dirname(full_local_file_path), exist_ok=True)
                        try:
                            blob.download_to_filename(full_local_file_path)
                        except Exception as e:
                            logging.error(f"Error downloading file: {e}")


            for file_path in self.file_paths:
                full_path = os.path.join('input', file_path)
                if not os.path.exists(full_path):
                    logging.error(f"File does not exist: {file_path}")
                    continue
                with open(full_path, 'r') as file:
                    input_text = file.read()
                clean_text = self.remove_trash(input_text)
                file_name = os.path.basename(file_path)
                for word in clean_text.split():
                    if word not in words:
                        words[word] = []
                    words[word].append((word, file_name, 1))
            gcs_files = set()
            mapper_output_dir = f'mapper_{self.mapper_index}'
            os.makedirs(mapper_output_dir, exist_ok=True)
            for word, file_counts in words.items():
                reducer_index = self.fnv1a_hash(word) % self.num_reducers
                intermediate_file_name = f'm{self.mapper_index}r{reducer_index}.json'
                intermediate_file_path = os.path.join(mapper_output_dir, intermediate_file_name)
                gcs_files.add(intermediate_file_path)
                with open(intermediate_file_path, 'a') as file:
                    for word, file_name, count in file_counts:  
                        file.write(json.dumps((word, file_name, count)) + '\n')

            print(f"Mapper{self.mapper_index} started uploading files to gcs")
            with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_file = {executor.submit(self.upload_file_to_gcs, bucket, file_path): file_path for file_path in gcs_files}

                    for future in as_completed(future_to_file):
                        file_path = future_to_file[future]
                        try:
                            future.result()  # This will raise an exception if the future encountered any issues
                            # print(f"Successfully uploaded {file_path}")
                        except Exception as e:
                            print(f"Error with file {file_path}: {e}")

            print(f"Mapper{self.mapper_index} finished uploading files to gcs")
            
        except Exception as e:
            logging.error(f"Mapper {self.mapper_index} encountered an error: {e}")
        finally:
            self.completion_event.set()

    def fnv1a_hash(self, input_string):
        """32-bit FNV-1a hash function"""
        FNV_prime = 0x01000193
        hash = 0x811C9DC5

        for byte in input_string.encode('utf-8'):
            hash ^= byte
            hash = (hash * FNV_prime) & 0xffffffff
        return hash
    
    def upload_file_to_gcs(self, bucket, file_path, timeout=120, max_retries=3):
        for attempt in range(max_retries):
            try:
                blob = bucket.blob(file_path)
                # chunk_size for chunked upload
                blob.chunk_size = 5 * 1024 * 1024  
                blob.upload_from_filename(file_path, timeout=timeout)
                print(f"Mapper{self.mapper_index} Uploaded {file_path}")
                break
            except Exception as e:
                print(f"Error uploading {file_path}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff

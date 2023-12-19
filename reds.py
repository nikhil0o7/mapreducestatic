        
import redis
import os
import json
import logging

def fetch_word_data(r,word):
    if r.hexists('words', word):
        data = r.hget('words', word)
        return json.loads(data)
    else:
        return None


def run():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        print("Started Connecting to Redis...")
        r = redis.Redis(
                    host='redis-15947.c238.us-central1-2.gce.cloud.redislabs.com', port=15947,
                # username="default", # use your Redis user. More info https://redis.io/docs/management/security/acl/
                password="otA2f9Fe4sm8z7TpdCk2u0DS2looQCaV", # use your Redis password
                # ssl=False,
                )
    except Exception as e:
        logging.error(f"Error connecting to Redis: {e}")
    finally:
        print("Connected to Redis.")
    final_output_file = os.path.join('final_output', 'final_output.json')
    try:
        result = r.flushdb()
        r.set('map_reduce_manual_indexing_done', 'NO')
        r.set('map_reduce_processing', 'NO')
        # word = 'away'
        # data = r.get(word)

        # if data:
        #     print(data)
        # else:
        #     print(f"No data found for '{word}'")
        # keys = r.keys('*')
        # for key in keys:
        #     value = r.get(key)
        #     print(f"{key}: {value}")
        
    except Exception as e:
        logging.error(f"Error flushing Redis database: {e}")


if __name__ == "__main__":
    run()
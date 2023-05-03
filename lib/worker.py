import os
import datetime
import time
from multiprocessing import current_process
import requests
from loguru import logger
import psycopg2


# Main process requesting data
def process_worker(queue, is_queue_empty, url, logs_filename, db_config=None, db_table=None):
    process = current_process()
    logger.info(f"{process.name} starting ...")
    logger.info(f"URL template: {url}")

    if db_config is not None:
        con = psycopg2.connect(**db_config)
        con.set_session(autocommit=True)
        cur = con.cursor()

    # Waiting for feeder
    time.sleep(1)

    logger.add(logs_filename, enqueue=False, format="{time};{level};{message}")

    api_key = os.environ.get('DL_API_KEY')
    limit_default = int(os.environ.get('LIMIT_DEFAULT'))
    long_response = os.environ.get('LONG_RESPONSE')

    while True:
        if queue.empty():
            logger.info(f"{process.name}: queue is empty")
            if is_queue_empty:
                break
            else:
                time.sleep(0.1)
                continue

        entity = queue.get()
        response = requests.get(url.format(entity=entity, limit=limit_default, api_key=api_key))  # ,

        message = f"{process.name};{entity};{response.status_code};{response.elapsed}"
        # Catching 4xx / 5xx
        if response.status_code != 200:
            logger.error(message)
        # Catching heavy requests
        elif response.elapsed > datetime.timedelta(seconds=int(long_response)):
            logger.warning(message)
        # Common case
        else:
            logger.info(message)

        if db_config is not None:
            query = f"""
                    INSERT INTO {db_table} 
                    (process, entity, code, response_time, response_content) 
                    VALUES(%s, %s, %s, %s, %s)
                    """
            cur.execute(query, (process.name, entity, response.status_code, response.elapsed, response.text))

    logger.info(f"{process.name}: queue and feeder is empty, I`m done")

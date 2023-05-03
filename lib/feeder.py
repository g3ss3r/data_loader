import os
import time
from multiprocessing import current_process
from loguru import logger


# We need this process because of limitations of queue size
def process_feeder(queue, is_queue_empty, data, logs_filename):
    process = current_process()
    logger.add(logs_filename, enqueue=False, format="{time};{level};{message}")
    logger.info(f"{process.name} starting ...")

    batch_size = int(os.environ.get('BATCH_SIZE'))

    while len(data) > 0:
        if queue.empty():
            data_range_to = batch_size if len(data) > batch_size else len(data)

            for i in range(0, data_range_to):
                wallet = data.pop(0).strip()
                queue.put(wallet)

            logger.info(f"{process.name} -> {data_range_to} items loaded to queue")
            time.sleep(0.1)

    is_queue_empty = 1
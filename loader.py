import os
import datetime
import time

import argparse
from dotenv import load_dotenv

import multiprocessing
from multiprocessing import current_process
from multiprocessing import Process
from ctypes import c_int

import requests
from loguru import logger


# We need this process because of limitations of queue size
def process_feeder(queue, is_queue_empty, data):
    process = current_process()
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


# Main process requesting data
def process_main(queue, is_queue_empty, reports_folder):
    process = current_process()
    logger.info(f"{process.name} starting ...")

    # Waiting for feeder
    time.sleep(1)

    file_name = reports_folder + datetime.datetime.now().strftime("%Y%m%d_%H%M%S.log")
    logger.add(file_name, enqueue=False, format="{time};{level};{message}")

    api_key = os.environ.get('DL_API_KEY')
    limit_default = int(os.environ.get('LIMIT_DEFAULT'))
    url = os.environ.get('URL_TEST')
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
        response = requests.get(url.format(entity=entity, limit=limit_default, api_key=api_key))

        # TODO: add possibility to save received data
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

    logger.info(f"{process.name}: queue and feeder is empty, I`m done")


def main():
    logger.info("main()")

    load_dotenv()

    # Default params
    # TODO: add folders to repo
    workers_default = os.environ.get('WORKERS_COUNT')
    reports_folder = os.environ.get('REPORTS_FOLDER')

    # Adjusting arguments parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-w', '--workers', nargs='?', default=workers_default, help="Workers count")
    parser.add_argument('-f', '--file', nargs="?", help="File with wallets list")
    parser.add_argument('-r', '--reports', nargs='?', default=reports_folder, help="Reports folder")

    args = parser.parse_args()

    # Validate input workers count value
    try:
        workers_count = int(args.workers)
    except ValueError as e:
        workers_count = workers_default
        logger.warning(f"Can`t parse Int({args.workers}), using default workers_count = {workers_default}")
        exit(0)

    # Checking for wallet list file existance

    try:
        if args.file is None:
            raise IOError()

        file = open(args.file, 'r')
    except IOError as e:
        logger.error(f'Can`t open file "{args.file}"')
        exit(0)

    # Checking for reports folder
    if not os.path.isdir(args.reports):
        os.mkdir(args.reports)
        logger.info(f"Creating reports folder {args.reports}")

    logger.info("Preparing queue and feeder")
    queue = multiprocessing.Queue()
    processes = []
    is_queue_empty = multiprocessing.Value(c_int, 0)

    data = file.readlines()
    params = (queue, is_queue_empty, data)
    process = Process(target=process_feeder, args=params)
    process.name = 'feeder'
    process.start()
    processes.append(process)

    # Adjusting and starting workers
    logger.info("Creating workers")
    for i in range(workers_count):
        params = (queue, is_queue_empty, args.reports)
        process = Process(target=process_main, args=params)
        process.name = 'receiver-' + str(i)
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()

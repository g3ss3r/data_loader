import os
import datetime
import argparse
from dotenv import load_dotenv
import multiprocessing
from multiprocessing import Process
from ctypes import c_int
import psycopg2
from psycopg2 import OperationalError
from loguru import logger

from lib.feeder import process_feeder
from lib.worker import process_worker


def main():
    logger.info("main()")

    load_dotenv()

    # Load default params from env
    workers_default = os.environ.get('WORKERS_COUNT')
    logs_folder = os.environ.get('LOGS_FOLDER')

    # Adjusting arguments parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', nargs='?', help="URL template for requests from .env file")
    parser.add_argument('-w', '--workers', nargs='?', default=workers_default, help="Workers count")
    parser.add_argument('-j', '--jobs', nargs="?", help="File with jobs list")
    parser.add_argument('-d', '--db_table', nargs='?', default=False, help="Table name to store responses in database")

    args = parser.parse_args()

    # Check db connection
    db_config = None
    con = None
    if args.db_table is not False:
        db_config = {
            "host": os.environ.get("DB_PG_HOST"),
            "user": os.environ.get("DB_PG_USER"),
            "password": os.environ.get("DB_PG_PASSWORD"),
            "port": os.environ.get("DB_PG_PORT"),
            "dbname": os.environ.get("DB_PG_DATABASE"),
            "sslmode": os.environ.get("DB_PG_SSLMODE")
        }
        try:
            con = psycopg2.connect(**db_config)
        except OperationalError as err:
            logger.error("Connection test error: " + str(err))
            exit(0)

        con.set_session(autocommit=True)

        # Create log DB
        query = f"""
        CREATE TABLE IF NOT EXISTS {args.db_table} (
            ts timestamp NOT NULL DEFAULT now(),
            process varchar NOT NULL,
            entity varchar NOT NULL,
            code int4 NOT NULL,
            response_time time NOT NULL,
            response_content varchar NOT NULL
        );
        """
        cur = con.cursor()
        cur.execute(query)
        con.close()

    # Validate input URL alias
    if args.url is None:
        logger.error("URL alias must be provided (use -u param )")
        exit(0)

    url = os.environ.get(args.url)
    if url is None:
        logger.error("URL alias is not found in .env, available choices:")
        for key in os.environ:
            if "URL_" in key:
                print(f"- {key}")
        exit(0)

    # Validate input workers count value
    try:
        workers_count = int(args.workers)
    except ValueError as e:
        workers_count = workers_default
        logger.warning(f"Can`t parse Int({args.workers}), using default workers_count = {workers_default}")
        exit(0)

    # Checking for wallet list file existance
    try:
        if args.jobs is None:
            raise IOError()

        jobs = open(args.jobs, 'r')
    except IOError as e:
        logger.error(f'Can`t open jobs file "{args.jobs}"')
        exit(0)

    # Checking for logs folder
    if not os.path.isdir(logs_folder):
        os.mkdir(logs_folder)
        logger.info(f"Creating logs folder {args.logs}")

    logs_filename = logs_folder + datetime.datetime.now().strftime("%Y%m%d_%H%M%S.log")

    logger.info("Preparing queue and feeder")
    queue = multiprocessing.Queue()
    processes = []
    is_queue_empty = multiprocessing.Value(c_int, 0)

    data = jobs.readlines()
    params = (queue,           # Queue for pushing entities to processes
              is_queue_empty,  # Flag to signalize workers that queue is not reloading but real empty
              data,            # Data with entities from job file
              logs_filename)   # Filename for this launch log

    process = Process(target=process_feeder, args=params)
    process.name = 'feeder'
    process.start()
    processes.append(process)

    # Adjusting and starting workers
    logger.info("Creating workers")
    for i in range(workers_count):
        params = (queue,
                  is_queue_empty,
                  url,
                  logs_filename,
                  db_config,
                  args.db_table)
        process = Process(target=process_worker, args=params)
        process.name = 'receiver-' + str(i)
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()

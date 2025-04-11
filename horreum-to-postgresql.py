#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import argparse
import datetime
import logging
import logging.handlers
import requests
import time
import psycopg2
import psycopg2.extras
import json
import urllib3
import typing


def setup_logger(app_name, stderr_log_lvl):
    """
    Create logger that logs to both stderr and log file but with different log levels
    """
    # Remove all handlers from root logger if any
    logging.basicConfig(
        level=logging.NOTSET, handlers=[]
    )  # `force=True` was added in Python 3.8 :-(
    # Change root logger level from WARNING (default) to NOTSET in order for all messages to be delegated
    logging.getLogger().setLevel(logging.NOTSET)

    # Log message format
    formatter = logging.Formatter(
        "%(asctime)s %(name)s %(threadName)s %(levelname)s %(message)s"
    )
    formatter.converter = time.gmtime

    # Silence loggers of some chatty libraries we use
    urllib_logger = logging.getLogger("urllib3.connectionpool")
    urllib_logger.setLevel(logging.WARNING)

    # Add stderr handler, with provided level
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(stderr_log_lvl)
    logging.getLogger().addHandler(console_handler)

    # Add file rotating handler, with level DEBUG
    rotating_handler = logging.handlers.RotatingFileHandler(
        filename=f"/tmp/{app_name}.log", maxBytes=100 * 1000, backupCount=2
    )
    rotating_handler.setFormatter(formatter)
    rotating_handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(rotating_handler)

    return logging.getLogger(app_name)


class Worker():
    def __init__(self):
        self.logger = logging.getLogger(str(self.__class__))

    def _setup(self, args):
        # FIXME
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.headers = {
            "Content-Type": "application/json",
            "X-Horreum-API-Key": args.horreum_api_token,
        }

    def _db_table_init(self, conn):
        """
        Checks if a table exists in a PostgreSQL database and creates it if it doesn't.

        Args:
            conn: A psycopg2 connection object.
        """
        cur = conn.cursor()
        cur.execute(f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'data');")
        table_exists = cur.fetchone()[0]

        if not table_exists:
            self.logger.info("Table not found in the PostgreSQL, creating it")
            cur.execute(f"""
                CREATE TABLE data (
                    id SERIAL PRIMARY KEY,
                    horreum_testid INTEGER,
                    horreum_runid INTEGER,
                    horreum_datasetid INTEGER,
                    start TIMESTAMP,
                    label_values JSONB,
                    UNIQUE (horreum_testid, horreum_runid, horreum_datasetid)
                );
            """)
            conn.commit()

    def _db_connect(self, args):
        """
        Connects to a PostgreSQL database.

        Args:
            args: Argparse namespace with PostgreSQL connection info.

        Returns:
            A psycopg2 connection object
        """
        db_params = {
            "dbname": args.postgresql_db,
            "user": args.postgresql_user,
            "password": args.postgresql_pass,
            "host": args.postgresql_host,
            "port": args.postgresql_port,
        }
        conn = psycopg2.connect(**db_params)
        self.logger.debug(f"Connected to the PostgreSQL host {db_params['host']}")
        return conn

    def _db_insert(self, conn, data_list):
        """
        Inserts a batch of data into the specified table, ignoring duplicate rows based on
        horreum_testid, horreum_runid, and horreum_datasetid.

        Args:
            conn: A psycopg2 connection object.
            data_list: A list of dictionaries, where each dictionary represents a row of data.
        """
        cur = conn.cursor()

        query = f"""
            INSERT INTO data (horreum_testid, horreum_runid, horreum_datasetid, start, label_values)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (horreum_testid, horreum_runid, horreum_datasetid) DO NOTHING;
        """

        values_list = []
        for i in data_list:
            values = (
                i["horreum_testid"],
                i["horreum_runid"],
                i["horreum_datasetid"],
                i["start"],
                json.dumps(i["label_values"]),
            )
            values_list.append(values)

        psycopg2.extras.execute_batch(cur, query, values_list)

        conn.commit()
        self.logger.debug(f"Inserted {len(data_list)} rows into PostgreSQL (duplicates ignored)")

    def _horreum_datasets(self, base_url: str, test_id: int, count: int, page: int, limit: int) -> typing.Generator[dict, None, None]:
        """
        Retrieves datasets from the Horreum API for a given test ID, handling pagination, acts as a generator.

        Args:
            base_url (str): The base URL of the Horreum.
            test_id (int): The ID of the test for which to retrieve datasets.
            count (int): The number of datasets to return at max.
            page (int): The starting page number for dataset retrieval.
            limit (int): The number of datasets to retrieve per page.

        Yields:
            dict: A dictionary representing a dataset from the Horreum API.
        """
        counter = 0
        while True:
            params = {
                "page": page,
                "limit": limit,
                "sort": "start",
                "direction": "Descending",
            }

            response = requests.get(
                f"{base_url}/api/dataset/list/{test_id}",
                headers=self.headers,
                params=params,
                verify=False,
            )
            response.raise_for_status()

            datasets = response.json().get("datasets", [])
            self.logger.debug(f"Loaded page {page} of datasets for test {test_id} with limit {limit}")

            for ds in datasets:
                yield ds

                counter += 1

                if counter >= count:
                    self.logger.debug(f"No more datasets is needed for test {test_id}")
                    return

            if len(datasets) == 0:
                self.logger.debug(f"Reached end of datasets for test {test_id} on page {page} with limit {limit}")
                return

            page += 1

    def _horreum_labelvalues(self, base_url: str, dataset_id: int) -> dict[str, str]:
        """
        Retrieves label values for a given dataset ID from the Horreum API.

        Args:
            base_url (str): The base URL of the Horreum API.
            dataset_id (int): The ID of the dataset for which to retrieve label values.

        Returns:
            dict: A dictionary containing label names as keys and label values as values.
        """
        response = requests.get(
            f"{base_url}/api/dataset/{dataset_id}/labelValues",
            headers=self.headers,
            verify=False,
        )
        response.raise_for_status()

        return {i["name"]: i["value"] for i in response.json()}

    def upload(self, args):
        self._setup(args)

        db_conn = self._db_connect(args)
        self._db_table_init(db_conn)

        datasets_generator = self._horreum_datasets(args.horreum_base_url, args.horreum_test_id, args.horreum_count, args.horreum_page, args.horreum_limit)
        data_to_insert = []

        for ds in datasets_generator:
            label_values = self._horreum_labelvalues(args.horreum_base_url, ds["id"])

            when = datetime.datetime.fromtimestamp(ds["start"] / 1000, datetime.UTC)

            self.logger.debug(f"Collected {len(label_values)} labels for dataset {ds['id']} from {when}")

            data_to_insert.append({
                "horreum_testid": ds["testId"],
                "horreum_runid": ds["runId"],
                "horreum_datasetid": ds["id"],
                "start": when,
                "label_values": label_values,
            })

            if len(data_to_insert) >= 10:
                self._db_insert(db_conn, data_to_insert)
                data_to_insert = []

        if len(data_to_insert) > 0:
            self._db_insert(db_conn, data_to_insert)

        db_conn.close()

    def set_args(self, parser, subparsers):
        parser.add_argument(
            "--horreum-base-url",
            default="https://horreum.corp.redhat.com",
            help="Base URL of Horreum server",
        )
        parser.add_argument(
            "--horreum-api-token",
            required=True,
            help="Horreum API token",
        )
        parser.add_argument(
            "--postgresql-host",
            required=True,
            help="PostgreSQL server host",
        )
        parser.add_argument(
            "--postgresql-port",
            type=int,
            default=5432,
            help="PostgreSQL server port",
        )
        parser.add_argument(
            "--postgresql-user",
            required=True,
            help="PostgreSQL username",
        )
        parser.add_argument(
            "--postgresql-pass",
            required=True,
            help="PostgreSQL password",
        )
        parser.add_argument(
            "--postgresql-db",
            required=True,
            help="PostgreSQL database",
        )

        # Options for listing results
        parser_upload = subparsers.add_parser(
            "upload",
            help="Morror data from Horreum to PostgreSQL",
        )
        parser_upload.set_defaults(func=self.upload)
        parser_upload.add_argument(
            "--horreum-test-id",
            type=int,
            required=True,
            help="Test ID from Horreum",
        )
        parser_upload.add_argument(
            "--horreum-count",
            type=int,
            default=100,
            help="How many datasets to process",
        )
        parser_upload.add_argument(
            "--horreum-limit",
            type=int,
            default=10,
            help="How many newest datasets to load on one page",
        )
        parser_upload.add_argument(
            "--horreum-page",
            type=int,
            default=1,
            help="From what page of datasets to start",
        )

### Params of the script
# --horreum-base-url
# --horreum-api-key
# --horreum-test-id
# --postgresql-host
# --postgresql-user
# --postgresql-pass

### Schema in PostgreSQL
# id - uniqe id
# horreum_testid - Test ID in Horreum
# horreum_runid -
# horreum_datasetid -
# start - datetime
# label_values - jsonb
#   {
#     "key1": 1,
#     "key2": 2,
#     ...
#   }

### Upload
# List datasets for given test ID, sorted by date with given limit (/api/dataset/list/{testId})
# For each dataset ID get data and ensure it is in PostgreSQL (/api/dataset/{datasetId}/labelValues)

### Cleanup
# For each of selected dataset ensure they are in Horreum and if not, delete them


def main():
    parser = argparse.ArgumentParser(
        description="Utility to mirror data from Horreum to PostgreSQL",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show verbose output",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Show debug output",
    )
    subparsers = parser.add_subparsers(
        help="sub-command help",
        required=True,
    )
    worker = Worker()
    worker.set_args(parser, subparsers)
    args = parser.parse_args()

    logger_name = "horreum-to-postgresql"
    if args.debug:
        logger = setup_logger(logger_name, logging.DEBUG)
    elif args.verbose:
        logger = setup_logger(logger_name, logging.INFO)
    else:
        logger = setup_logger(logger_name, logging.WARNING)

    logger.debug(f"Args: {args}")

    worker.upload(args)


if __name__ == "__main__":
    main()

#!/usr/bin/env python
# coding=utf-8
from __future__ import absolute_import
from __future__ import print_function

import gzip
import json
import multiprocessing
import os
import sys
from time import sleep

from pynamodb.connection import Connection
from .batch import BatchPutManager

config = dict()


def load_table(host, region, table_name, parallelism=1, load_files=None):
    if load_files is None:
        load_files = []
    connection = Connection(host=host, region=region)
    desc = connection.describe_table(table_name)

    if desc is None:
        sys.stderr.writelines([
            "Table does not exist - please create first (you can use the official AWS CLI tool to do this).\n"
        ])
        sys.exit(-1)

    check_files(load_files)

    desc = connection.describe_table(table_name)

    queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(
        processes=parallelism,
        initializer=pool_init,
        initargs=[queue, host, region, table_name]
    )
    for filename in load_files:
        pool.apply_async(load_part, [filename])

    files_completed = 0
    items_loaded = 0
    while True:
        sleep(1)
        while not queue.empty():
            update = queue.get()
            if update == 'complete':
                files_completed += 1
            else:
                items_loaded += update

        print("{} items loaded - {}/{} files complete.".format(
            items_loaded,
            files_completed,
            len(load_files)
        ))
        sys.stdout.flush()

        if files_completed == len(load_files):
            break

    pool.close()
    pool.join()

    print("Done.")


def check_files(load_files):
    if not len(load_files):
        sys.stderr.writelines([
            "No files to load from.\n"
        ])
        raise SystemExit(1)

    for load_file in load_files:
        if not os.path.exists(load_file):
            sys.stderr.writelines([
                "File {} does not exist.\n".format(load_file)
            ])
            raise SystemExit(1)


def pool_init(queue, host, region, table_name):
    config['queue'] = queue
    config['host'] = host
    config['region'] = region
    config['table_name'] = table_name


def load_part(filename):
    try:
        connection = Connection(host=config['host'], region=config['region'])

        if filename.endswith('.gz'):
            opener = gzip.GzipFile
        else:
            opener = open

        with opener(filename, 'r') as infile:
            with BatchPutManager(connection, config['table_name']) as batch:
                for line in infile:
                    item = json.loads(line)
                    batch.put(item)
                    config['queue'].put(1)

        config['queue'].put('complete')
    except Exception as e:
        print('Unhandled exception: {0}'.format(e))

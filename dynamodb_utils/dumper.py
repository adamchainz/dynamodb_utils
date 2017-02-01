#!/usr/bin/env python
# coding=utf-8
from __future__ import absolute_import
from __future__ import print_function

import argparse
import gzip
import json
import multiprocessing
import sys
from time import sleep

from pynamodb.connection import Connection
from pynamodb.constants import (ITEMS, LAST_EVALUATED_KEY,
                                PROVISIONED_THROUGHPUT, READ_CAPACITY_UNITS,
                                TOTAL)
from .batch import BatchDumper

config = dict();

def dump_table(host, region, table_name, total_segments=1, hash_keys=[], compress=False, parallelism=1, capacity_consumption=0.5):
    capacity_consumption = max(0.01, capacity_consumption)
    capacity_consumption = min(1.0, capacity_consumption)

    connection = Connection(host=host, region=region)
    desc = connection.describe_table(table_name)
    if desc is None:
        sys.stderr.writelines(["Table does not exist."])
        sys.exit(-1)
    total_items = desc['ItemCount']

    total_capacity = desc[PROVISIONED_THROUGHPUT][READ_CAPACITY_UNITS]
    capacity_per_process = max(
        1.0,
        (capacity_consumption * total_capacity) / float(parallelism)
    )

    queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(
        processes=parallelism,
        initializer=pool_init,
        initargs=[queue, capacity_per_process, host, region, compress, table_name]
    )

    # Keys or segments?
    if len(hash_keys):
        for key in hash_keys:
            pool.apply_async(dump_part, [key])
        num_to_do = len(hash_keys)
    else:
        for segment in range(total_segments):
            pool.apply_async(dump_part, [segment, total_segments])
        num_to_do = total_segments

    num_complete = 0
    items_dumped = 0
    while True:
        sleep(1)
        while not queue.empty():
            update = queue.get()
            if update == 'complete':
                num_complete += 1
            else:
                items_dumped += update

        print("{}/~{} items dumped - {}/{} {}.".format(
            items_dumped,
            total_items,
            num_complete,
            num_to_do,
            "keys" if len(hash_keys) else "segments"
        ))

        if num_complete == num_to_do:
            break

    pool.close()
    pool.join()

    print("Done.")


def pool_init(queue, capacity, host, region, compress, table_name):
    config['queue'] = queue
    config['capacity'] = capacity
    config['host'] = host
    config['region'] = region
    config['compress'] = compress
    config['table_name'] = table_name


def dump_part(part, total_segments=None):
    """
    'part' may be the hash_key if we are dumping just a few hash_keys - else
    it will be the segment number
    """
    try:
        connection = Connection(host=config['host'], region=config['region'])

        filename = ".".join([config['table_name'], str(part), "dump"])
        if config['compress']:
            opener = gzip.GzipFile
            filename += ".gz"
        else:
            opener = open

        dumper = BatchDumper(connection, config['table_name'], config['capacity'], part, total_segments)

        with opener(filename, 'w') as output:
            while dumper.has_items:
                items = dumper.get_items()

                for item in items:
                    output.write(json.dumps(item))
                    output.write("\n")
                output.flush()
                config['queue'].put(len(items))
        config['queue'].put('complete')
    except Exception as e:
        print('Unhandled exception: {0}'.format(e))

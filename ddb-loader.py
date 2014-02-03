#!/usr/bin/env python
# coding=utf-8
import argparse
import gzip
import json
import multiprocessing
import sys
from time import sleep

from pynamodb.connection import Connection, TableConnection
from pynamodb.constants import (BATCH_WRITE_PAGE_LIMIT, PUT_REQUEST,
                                UNPROCESSED_KEYS)
from pynamodb.models import Model
from pynamodb.attributes import NumberAttribute, UnicodeAttribute


parser = argparse.ArgumentParser(
    prog="ddb-loader",
    description="""DynamoDB Loader: restore tables dumped by ddb-dumper with ease."""
)
parser.add_argument(
    '-r',
    '--region',
    type=str,
    default="us-east-1",
    help="The region to connect to."
)
parser.add_argument(
    '-s',
    '--host',
    type=str,
    help="The host url to connect to (for use with DynamoDB Local)."
)
parser.add_argument(
    '-l',
    '--load-files',
    type=str,
    nargs='*',
    required=True,
    help="The list of filenames of dump files created by ddb-dumper that you wish to load."
)
parser.add_argument(
    '--hash-key',
    type=str,
    default=None,
    help="If the table is to be created, specifies the hash key. Uses the format 'type:name' e.g. number:user_id, or string:username."
)
parser.add_argument(
    '--range-key',
    type=str,
    default=None,
    help="If the table is to be created, specifies the range key. Uses the  format 'type:name' e.g. number:user_id, or string:username."
)
parser.add_argument(
    'table_name',
    type=str,
    help="The name of the table to load into."
)


def main(host, region, table_name, load_files, hash_key, range_key):
    connection = Connection(host=host, region=region)
    desc = connection.describe_table(table_name)

    if desc is None:
        print "Table does not exist - creating table '{}'...".format(table_name)

        table_kwargs = {
            'table_name': table_name
        }
        name, attr = parse_hash_key(hash_key)
        table_kwargs[name] = attr

        if range_key:
            name, attr = parse_range_key(range_key)
            table_kwargs[name] = attr

        # Dynamically create table model
        table = type('table', (Model,), table_kwargs)
        table.connection = TableConnection(table_name=table_name, host=host, region=region)
        table.create_table(
            read_capacity_units=1000,
            write_capacity_units=1000,
            wait=True
        )

    desc = connection.describe_table(table_name)

    queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(
        initializer=load_init,
        initargs=(queue,)
    )
    for filename in load_files:
        pool.apply_async(load_file, [host, region, table_name, filename])

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

        print "{} items loaded - {}/{} files complete.".format(
            items_loaded,
            files_completed,
            len(load_files)
        )
        sys.stdout.flush()

        if files_completed == len(load_files):
            break

    pool.close()
    pool.join()

    print "Done."


def parse_hash_key(hash_key):
    if not hash_key:
        print "You must specify a hash key with --hash-key when the table needs creating."
        raise SystemExit(-1)

    if not ':' in hash_key or len(hash_key.split(':')) != 2:
        print "Hash key did not match format 'type:name' e.g. number:user_id"
        raise SystemExit(-1)

    the_type, name = hash_key.split(':')

    if the_type == 'number':
        attr = NumberAttribute(hash_key=True)
    elif the_type == 'string':
        attr = UnicodeAttribute(hash_key=True)
    else:
        print "Hash key type should be 'string' or 'number'."

    return name, attr


def parse_range_key(range_key):
    if not ':' in range_key or len(range_key.split(':')) != 2:
        print "Range key did not match format 'type:name' e.g. number:user_id"
        raise SystemExit(-1)

    the_type, name = range_key.split(':')

    if the_type == 'number':
        attr = NumberAttribute(range_key=True)
    elif the_type == 'string':
        attr = UnicodeAttribute(range_key=True)
    else:
        print "Range key type should be 'string' or 'number'."

    return name, attr


def load_init(queue):
    proc = multiprocessing.current_process()
    proc.queue = queue


class BatchPutManager(object):
    def __init__(self, connection, table_name):
        self.connection = connection
        self.table_name = table_name
        self.max_operations = BATCH_WRITE_PAGE_LIMIT
        self.items = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.commit()

    def put(self, item):
        self.items.append(item)
        if len(self.items) == 25:
            self.commit()

    def commit(self):
        if not len(self.items):
            return

        unprocessed_keys = [{PUT_REQUEST: item} for item in self.items]

        while unprocessed_keys:
            items = []
            for key in unprocessed_keys:
                items.append(key.get(PUT_REQUEST))
            data = self.connection.batch_write_item(
                table_name=self.table_name,
                put_items=items,
            )
            unprocessed_keys = data.get(UNPROCESSED_KEYS, {}).get(self.table_name)

        self.items = []


def load_file(host, region, table_name, filename):
    proc = multiprocessing.current_process()
    queue = proc.queue

    connection = Connection(host=host, region=region)

    if filename.endswith('.gz'):
        opener = gzip.GzipFile
    else:
        opener = open

    with opener(filename, 'r') as infile:
        with BatchPutManager(connection, table_name) as batch:
            for line in infile:
                item = json.loads(line)
                batch.put(item)
                queue.put(1)

    queue.put('complete')


if __name__ == '__main__':
    kwargs = dict(parser.parse_args(sys.argv[1:])._get_kwargs())
    main(**kwargs)

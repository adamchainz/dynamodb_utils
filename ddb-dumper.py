#!/usr/bin/env python
# coding=utf-8
import argparse
import gzip
import json
import multiprocessing
import sys
from time import sleep

from pynamodb.connection import Connection
from pynamodb.constants import ITEMS, LAST_EVALUATED_KEY


parser = argparse.ArgumentParser(
    prog="ddb-dumper",
    description="""DynamoDB Dumper: backup tables out of DynamoDB with ease."""
)
parser.add_argument(
    '-r',
    '--region',
    type=str,
    default="us-east-1",
    help="The region to connect to."
)
parser.add_argument(
    '--host',
    type=str,
    help="The host url to connect to (for use with DynamoDB Local)."
)
parser.add_argument(
    '-s',
    '--total-segments',
    type=int,
    default=10,
    help="The number of segments to scan in parallel (default 10)."
)
parser.add_argument(
    '-c',
    '--compress',
    action='store_true',
    help="Whether output files should be compressed with gzip (default off)."
)
parser.add_argument(
    'table_name',
    type=str,
    help="The name of the table to dump."
)


def main(host, region, table_name, total_segments, compress):
    connection = Connection(host=host, region=region)
    desc = connection.describe_table(table_name)
    if desc is None:
        raise NameError("Table does not exist.")
    total_items = desc['ItemCount']

    queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(processes=total_segments,
                                initializer=dump_init,
                                initargs=(queue,))
    for x in xrange(total_segments):
        pool.apply_async(dump, [host, region, table_name, x, total_segments, compress])

    segments_complete = 0
    items_dumped = 0
    while True:
        sleep(1)
        while not queue.empty():
            update = queue.get()
            if update == 'complete':
                segments_complete += 1
            else:
                items_dumped += update

        print "{}/~{} items dumped - {}/{} segments.".format(
            items_dumped,
            total_items,
            segments_complete,
            total_segments,
        )

        if segments_complete == total_segments:
            break

    pool.close()
    pool.join()

    print "Done."


def dump_init(_queue):
    multiprocessing.current_process().queue = _queue


def dump(host, region, table_name, segment, total_segments, compress):
    queue = multiprocessing.current_process().queue
    connection = Connection(host=host, region=region)

    filename = ".".join([table_name, str(segment), str(total_segments), "dump"])
    if compress:
        opener = gzip.GzipFile
        filename += ".gz"
    else:
        opener = open

    with opener(filename, 'w') as output:
        data = None
        last_evaluated_key = None
        while data is None or last_evaluated_key:
            data = connection.scan(
                table_name=table_name,
                segment=segment,
                limit=100,
                total_segments=total_segments,
                exclusive_start_key=last_evaluated_key
            )
            items = data.get(ITEMS)

            for item in items:
                output.write(json.dumps(item))
                output.write("\n")
            output.flush()

            queue.put(len(items))
            sleep(0.1)
            last_evaluated_key = data.get(LAST_EVALUATED_KEY)

    queue.put('complete')


if __name__ == '__main__':
    kwargs = dict(parser.parse_args(sys.argv[1:])._get_kwargs())
    main(**kwargs)

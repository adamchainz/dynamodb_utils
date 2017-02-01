from __future__ import absolute_import
import argparse
import multiprocessing
from botocore.session import get_session
from . import dumper, loader

def dump():
    parser = argparse.ArgumentParser(
        prog="dynamodb-dumper",
        description="""DynamoDB Dumper: backup tables out of DynamoDB with ease."""
    )
    parser.add_argument(
        '-r',
        '--region',
        type=str,
        default=get_session().get_config_variable('region'),
        help="The region to connect to."
    )
    parser.add_argument(
        '-o',
        '--host',
        type=str,
        help="The host url to connect to (for use with DynamoDB Local)."
    )
    parser.add_argument(
        '-s',
        '--total-segments',
        type=int,
        default=multiprocessing.cpu_count(),
        help="The number of segments to scan in parallel (defaults to the number of processors you have)."
    )
    parser.add_argument(
        '-k',
        '--hash-keys',
        type=str,
        nargs='+',
        default=[],
        help="The list of hash keys to dump data for (defaults to all - by setting this, 'Query' will be used as opposed to 'Scan' with parallel segments - thus --total-segments will be overridden)."
    )
    parser.add_argument(
        '-p',
        '--parallelism',
        type=int,
        default=multiprocessing.cpu_count(),
        help="The number of processes to use (defaults to the number of processors you have)."
    )
    parser.add_argument(
        '-c',
        '--compress',
        action='store_true',
        help="Whether output files should be compressed with gzip (default off)."
    )
    parser.add_argument(
        '--capacity-consumption',
        type=float,
        default=0.5,
        help="The amount (between 0.01 and 1.0) of the total read capacity of the table to consume (default 0.5)."
    )
    parser.add_argument(
        'table_name',
        type=str,
        help="The name of the table to dump."
    )
    kwargs = vars(parser.parse_args())
    dumper.dump_table(**kwargs)

def load():
    parser = argparse.ArgumentParser(
        prog="dynamodb-loader",
        description="""DynamoDB Loader: restore tables dumped by dynamodb-dumper with ease."""
    )
    parser.add_argument(
        '-r',
        '--region',
        type=str,
        default=get_session().get_config_variable('region'),
        help="The region to connect to."
    )
    parser.add_argument(
        '-o',
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
        '-p',
        '--parallelism',
        type=int,
        default=multiprocessing.cpu_count(),
        help="The number of processes to use (defaults to the number of processors you have)."
    )
    parser.add_argument(
        'table_name',
        type=str,
        help="The name of the table to load into."
    )
    kwargs = vars(parser.parse_args())
    loader.load_table(**kwargs)

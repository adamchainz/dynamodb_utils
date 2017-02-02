from __future__ import print_function

import random
from unittest import TestCase

from botocore.vendored.requests import ConnectionError
from pynamodb.connection import Connection
from pynamodb.exceptions import TableError
from pynamodb.types import HASH, STRING


class DynamoDBLocalTestCase(TestCase):
    DDB_LOCAL_URL = 'localhost'
    DDB_LOCAL_PORT = 3232
    DDB_LOCAL_HOST = 'http://{}:{}'.format(DDB_LOCAL_URL, DDB_LOCAL_PORT)
    DDB_LOCAL_REGION = 'localhost'

    @classmethod
    def setUpClass(cls):
        cls.connect()

    @classmethod
    def connect(cls):
        cls.connection = Connection(host=cls.DDB_LOCAL_HOST, region=cls.DDB_LOCAL_REGION)

        try:
            cls.connection.list_tables()
        except (TableError, ConnectionError) as e:
            print(e)
            raise EnvironmentError(
                "DynamoDB Local does not appear to be running at {}! Cannot run tests!".format(cls.DDB_LOCAL_HOST)
            )

    def transient_table(self, **kwargs):
        return TransientLocalTable(self.connection, **kwargs)

    simple_table = dict(
        attribute_definitions=[{
            'attribute_type': STRING,
            'attribute_name': 'object_id'
        }],
        key_schema=[{
            'key_type': HASH,
            'attribute_name': 'object_id'
        }]
    )


class TransientLocalTable(object):
    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.table_name = self.unique_table_name()
        self.creation_kwargs = kwargs

    def unique_table_name(self):
        existing = set(self.connection.list_tables()['TableNames'])
        while True:
            table_name = 'test-table-{}'.format(random.randint(0, 999999))
            if table_name not in existing:
                return table_name

    def __enter__(self):
        self.connection.create_table(
            table_name=self.table_name,
            read_capacity_units=10,
            write_capacity_units=10,
            **self.creation_kwargs
        )
        return self.table_name

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.delete_table(table_name=self.table_name)

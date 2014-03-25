import os
import subprocess

from .base import DynamoDBLocalTestCase

from pynamodb.types import HASH, STRING


THIS_DIR = os.path.abspath(os.path.dirname(__file__))


class DynamoDBLoaderTests(DynamoDBLocalTestCase):

    def test_load_simple(self):
        table_creation_kwargs = dict(
            attribute_definitions=[{
                'attribute_type': STRING,
                'attribute_name': 'object_id'
            }],
            key_schema=[{
                'key_type': HASH,
                'attribute_name': 'object_id'
            }]
        )

        with self.transient_table(**table_creation_kwargs) as table_name:
            # Check 0 items first
            items = self.connection.scan(table_name=table_name)['Items']
            self.assertEqual(len(items), 0)

            # Run dynamodb-loader to load some items
            os.chdir(THIS_DIR)
            subprocess.check_output(
                ['../bin/dynamodb-loader',
                 table_name,
                 '--host', self.DDB_LOCAL_HOST,
                 '--region', self.DDB_LOCAL_REGION,
                 '--parallelism', '1',
                 '--load', 'loader-test-simple.dump'],
            )

            # Check they are loaded
            items = self.connection.scan(table_name=table_name)['Items']
            self.assertEqual(len(items), 1)

            self.assertDictEqual(items[0], {
                'object_id': {'S': 'DEADBEEF'},
                'foo': {'N': '802'}
            })

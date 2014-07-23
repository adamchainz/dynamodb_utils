import os
import subprocess

from .base import DynamoDBLocalTestCase


THIS_DIR = os.path.abspath(os.path.dirname(__file__))


class DynamoDBLoaderTests(DynamoDBLocalTestCase):

    def test_load_simple(self):
        with self.transient_table(**self.simple_table) as table_name:
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
            self.assertEqual(len(items), 2)

            items.sort(key=lambda i: i['object_id']['S'])

            self.assertEqual(items[0], {
                'object_id': {'S': 'DEADBABE'},
                'foo': {'N': '424'}
            })

            self.assertEqual(items[1], {
                'object_id': {'S': 'DEADBEEF'},
                'foo': {'N': '802'}
            })

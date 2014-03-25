import json
import os
import subprocess

from .base import DynamoDBLocalTestCase

from pynamodb.types import HASH, STRING


THIS_DIR = os.path.abspath(os.path.dirname(__file__))


class DynamoDBDumperTests(DynamoDBLocalTestCase):

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
            # Create item to dump
            self.connection.put_item(
                table_name=table_name,
                hash_key='DEADBEEF',
                attributes={
                    'foo': {'N': '802'}
                }
            )

            items = self.connection.scan(table_name=table_name)['Items']
            self.assertEqual(len(items), 1)

            # Run dynamodb-dumper
            os.chdir(THIS_DIR)
            subprocess.check_output(
                ['../bin/dynamodb-dumper',
                 table_name,
                 '--host', self.DDB_LOCAL_HOST,
                 '--region', self.DDB_LOCAL_REGION,
                 '--total-segments', '1'],
            )

            # Check a .dump file was created
            dump_name = [name for name in os.listdir(THIS_DIR)
                         if (name.startswith(table_name) and
                             name.endswith('.dump'))][0]

            with open(dump_name, 'r') as dumpfile:
                lines = [line for line in dumpfile]

            os.unlink(dump_name)

            self.assertEqual(len(lines), 1)
            item = json.loads(lines[0])
            self.assertDictEqual(item['object_id'], {'S': 'DEADBEEF'})
            self.assertDictEqual(item['foo'], {'N': '802'})

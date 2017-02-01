import json
import sys
import os

from .base import DynamoDBLocalTestCase
from dynamodb_utils import cli
from mock import patch

THIS_DIR = os.path.abspath(os.path.dirname(__file__))


class DynamoDBDumperTests(DynamoDBLocalTestCase):

    def test_dump_simple(self):
        with self.transient_table(**self.simple_table) as table_name:
            # Create items to dump
            self.connection.put_item(
                table_name=table_name,
                hash_key='CAFEBABE',
                attributes={'foo': {'N': '424'}}
            )
            self.connection.put_item(
                table_name=table_name,
                hash_key='DEADBEEF',
                attributes={
                    'foo': {'N': '802'}
                }
            )

            items = self.connection.scan(table_name=table_name)['Items']
            self.assertEqual(len(items), 2)

            # Run dynamodb-dumper
            os.chdir(THIS_DIR)
            args = ['dynamodb-dumper',
                    table_name,
                    '--host', self.DDB_LOCAL_HOST,
                    '--region', self.DDB_LOCAL_REGION,
                    '--total-segments', '1',
            ]
            with patch.object(sys, 'argv', args):
                cli.dump() 

            # Check a .dump file was created
            dump_name = [name for name in os.listdir(THIS_DIR)
                         if (name.startswith(table_name) and
                             name.endswith('.dump'))][0]

            with open(dump_name, 'r') as dumpfile:
                lines = [line for line in dumpfile]

            os.unlink(dump_name)

            self.assertEqual(len(lines), 2)
            items = [json.loads(line) for line in lines]
            items.sort(key=lambda i: i['object_id']['S'])

            self.assertEqual(
                items[0],
                {"object_id": {"S": "CAFEBABE"}, "foo": {"N": "424"}}
            )
            self.assertEqual(
                items[1],
                {"object_id": {"S": "DEADBEEF"}, "foo": {"N": "802"}}
            )

    def test_dump_hash_key(self):
        with self.transient_table(**self.simple_table) as table_name:
            # Create items
            self.connection.put_item(
                table_name=table_name,
                hash_key='CAFEBABE',
                attributes={'foo': {'N': '424'}}
            )
            self.connection.put_item(
                table_name=table_name,
                hash_key='DEADBEEF',
                attributes={'foo': {'N': '802'}}
            )

            items = self.connection.scan(table_name=table_name)['Items']
            self.assertEqual(len(items), 2)

            # Run dynamodb-dumper
            os.chdir(THIS_DIR)
            args = ['dynamodb-dumper',
                    table_name,
                    '--host', self.DDB_LOCAL_HOST,
                    '--region', self.DDB_LOCAL_REGION,
                    '--total-segments', '1',
                    '--hash-keys', 'CAFEBABE',
            ]
            with patch.object(sys, 'argv', args):
                cli.dump() 

            # Check a .dump file was created
            dump_name = [name for name in os.listdir(THIS_DIR)
                         if (name.startswith(table_name) and
                             name.endswith('.dump'))][0]

            with open(dump_name, 'r') as dumpfile:
                lines = [line for line in dumpfile]

            os.unlink(dump_name)

            self.assertEqual(len(lines), 1)
            item = json.loads(lines[0])
            self.assertEqual(
                item,
                {"object_id": {"S": "CAFEBABE"}, "foo": {"N": "424"}}
            )

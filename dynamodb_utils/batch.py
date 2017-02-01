from pynamodb.constants import (ITEMS, TOTAL, UNPROCESSED_KEYS, LAST_EVALUATED_KEY,
                                PROVISIONED_THROUGHPUT, READ_CAPACITY_UNITS,
                                PUT_REQUEST, BATCH_WRITE_PAGE_LIMIT)
from pynamodb.throttle import Throttle

class BatchDumper(object):
    def __init__(self, connection, table_name, capacity, part, total_segments):
        self.connection = connection
        self.table_name = table_name
        self.throttle = Throttle(capacity)
        self.part = part
        self.total_segments = total_segments
        self.has_items = True
        self.last_evaluated_key = None

    def get_items(self):
        data = self.get_data()

        capacity = data.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
        self.throttle.add_record(capacity)

        self.last_evaluated_key = data.get(LAST_EVALUATED_KEY)
        self.has_items = (self.last_evaluated_key is not None)

        return data.get(ITEMS)

    def get_data(self):
        if self.total_segments is not None:
            return self.connection.scan(
                table_name=self.table_name,
                segment=self.part,
                total_segments=self.total_segments,
                exclusive_start_key=self.last_evaluated_key,
                return_consumed_capacity=TOTAL,
                limit=100,
            )
        else:
            return self.connection.query(
                table_name=self.table_name,
                hash_key=self.part,
                exclusive_start_key=self.last_evaluated_key,
                limit=100
            )

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



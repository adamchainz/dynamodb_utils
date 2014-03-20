dynamodb_utils
==============

A toolchain for Amazon's `DynamoDB <http://aws.amazon.com/dynamodb/>`_ to make
common operations easier. Currently contains:

* ``dynamodb-dumper`` - backup tables out of DynamoDB with ease.
* ``dynamodb-loader`` - restore tables dumped by ``dynamodb-dumper`` with ease.

All tools are built to work with both the main DynamoDB service and DynamoDB
Local so you can test them out, and also move data easily between production
and your test environment.

Install with::

    pip install dynamodb_utils


Usage Examples
--------------

You can get the (hopefully detailed enough) help with e.g.::

    dynamodb-dumper --help

To dump a table, with compression::

    dynamodb-dumper mytable.name --compress

To dump just a few hash-key values from a table (for example, if you want a
restricted subset of data for development)::

    dynamodb-dumper mytable.name --compress --hash-keys 101 104 404

To load some compressed dump files on a DynamoDB Local instance running locally
on port 3232::

    dynamodb-loader mytable.name --host http://localhost:3232 --region localhost --load mytable.name.*.dump.gz

=========
ddb-utils
=========

A toolchain for Amazon's `DynamoDB <http://aws.amazon.com/dynamodb/>`_ to make
common operations easier. Currently contains:

* ``ddb-dumper`` - backup tables out of DynamoDB with ease.
* ``ddb-loader`` - restore tables dumped by ``ddb-dumper`` with ease.

All tools are built to work with both the main DynamoDB service and DynamoDB
Local so you can test them out, and also move data easily between production
and your test environment.

Install with:

    pip install ddb-utils

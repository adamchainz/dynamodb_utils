dynamodb_utils
==============

A toolchain for Amazon's `DynamoDB <http://aws.amazon.com/dynamodb/>`_ to make
common operations easier. Currently contains:

* ``dynamodb_dumper`` - backup tables out of DynamoDB with ease.
* ``dynamodb_loader`` - restore tables dumped by ``dynamodb_dumper`` with ease.

All tools are built to work with both the main DynamoDB service and DynamoDB
Local so you can test them out, and also move data easily between production
and your test environment.

Install with::

    $ pip install dynamodb_utils

List the documentation for the tools with e.g.::

    dynamodb-dumper --help

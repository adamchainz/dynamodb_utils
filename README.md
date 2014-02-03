ddb-utils
=========

A toolchain for AWS DynamoDB to make common operations easier. Currently
contains:

* `ddb-dumper` - backup tables out of DynamoDB with ease.
* `ddb-loader` - restore tables dumpe by `ddb-dumper` with ease.

All tools are built to work with both the main DynamoDB service and DynamoDB
Local so you can test them out, and also move data easily between production
and your test environment.

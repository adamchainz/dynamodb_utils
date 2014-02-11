from setuptools import setup


setup(
    name='dynamodb_utils',
    version='0.2',
    author='Adam Johnson',
    author_email='me@adamj.eu',
    url='https://github.com/adamchainz/dynamodb_utils',
    description='A toolchain for AWS DynamoDB to make common operations easier.',
    long_description=open('README.rst').read(),
    license='GPLv3',
    install_requires=[
        'pynamodb'
    ],
    keywords='python dynamodb amazon aws',
    scripts=[
        'bin/dynamodb_dumper',
        'bin/dynamodb_loader',
    ]
)

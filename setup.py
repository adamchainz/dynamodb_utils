from setuptools import setup


setup(
    name='ddb-utils',
    version='0.1',
    author='Adam Johnson',
    author_email='me@adamj.eu',
    url='https://github.com/adamchainz/ddb-utils',
    description='A toolchain for AWS DynamoDB to make common operations easier.',
    long_description=open('README.rst').read(),
    license='GPLv3',
    install_requires=[
        'pynamodb'
    ],
    keywords='python dynamodb amazon aws',
    scripts=[
        'bin/ddb-dumper',
        'bin/ddb-loader',
    ]
)

from setuptools import setup


setup(
    name='dynamodb_utils',
    version='1.0.0',
    packages=['dynamodb_utils'],
    author='Adam Johnson',
    author_email='me@adamj.eu',
    url='https://github.com/adamchainz/dynamodb_utils',
    description='A toolchain for AWS DynamoDB to make common operations (backup, restore backups) easier.',
    long_description=open('README.rst').read(),
    license='MIT license',
    install_requires=[
        'pynamodb',
    ],
    keywords='python dynamodb amazon aws',
    entry_points={
        'console_scripts': [
           'dynamodb-dumper = dynamodb_utils.cli:dump',
           'dynamodb-loader = dynamodb_utils.cli:load'
        ]
    },
    classifiers=[
        'License :: OSI Approved :: ISC License (ISCL)',
    ]
)

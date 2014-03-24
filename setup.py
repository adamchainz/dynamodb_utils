from setuptools import setup


with open('requirements.txt') as reqs:
    install_requires = [line for line in reqs]

setup(
    name='dynamodb_utils',
    version='0.4',
    author='Adam Johnson',
    author_email='me@adamj.eu',
    url='https://github.com/adamchainz/dynamodb_utils',
    description='A toolchain for AWS DynamoDB to make common operations easier.',
    long_description=open('README.rst').read(),
    license='GPLv3',
    install_requires=install_requires,
    keywords='python dynamodb amazon aws',
    scripts=[
        'bin/dynamodb-dumper',
        'bin/dynamodb-loader',
    ]
)

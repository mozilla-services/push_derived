# Push Derived Logstreams & Analytics

This repo holds [hindsight](https://github.com/trink/hindsight) log shipping
for Mozilla Push Service logs and Python scripts for daily/monthly analytics.

## hindsight Processing

Generates Redshift derived streams from Mozilla Push server logs.

## Python Analytics

A Python package `analytics` runs queries for Push analytics and updates rollup
tables in Redshift.

### Installing

You will need postgresql development libraries for ``psycopg2`` to compile
properly (this is typically a libpq-dev package).

Installing this in a virtualenv is recommended, pipsi is an alternate installer
that handles the virtualenv automatically.

1. ``pip install -r requirements.txt``
2. ``python setup.py develop``

### Running

The script is installed as `push_metrics` and can be run with arguments passed
in or set as environment variables.

Environment Variables:

ROLLUP_DB_STRING
    SQLAlchemy ready database string. ie.
    postgresql+psycopg2://USERNAME:PASSWORD@HOSTNAME:5439/DB_NAME

ROLLUP_TABLE_PREFIX
    Prefix for autopush connection logs.

ROLLUP_DAYS_AGO
    How many prior days to process logs for.

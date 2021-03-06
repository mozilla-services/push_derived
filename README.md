# Push Derived Logstreams & Analytics

This repo holds [Hindsight](https://github.com/trink/hindsight) log shipping
for Mozilla Push Service logs and Python scripts for daily/monthly analytics.

## Hindsight Processing

Generates Redshift derived streams from Mozilla Push server logs.

## Python Analytics

A Python package `analytics` runs queries for Push analytics and updates rollup
tables in Redshift.

### Installing and Running

The `push_load.sh` script will install all required dependencies and run both
the Hindsight job for loading the most recent log files and the Python job for
updating the rollup tables. It is meant to be run on a brand new EC2 Centos
instance based on the Mozilla Services standard Centos AMI.

Before running the script, you must first ensure that the ec2-user's AWS
credentials are set up correctly to provide access to the `heka-logs` S3
buckets. Then you must visit the `hindsight/hs_run/output` folder and edit all
of the `*.cfg` files so that they contain the correct Redshift database
connection values. You also must edit the db_creds.sh file and do the
same. Unfortunately, yes, you have to do this repeatedly for each file.

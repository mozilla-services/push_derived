#!/usr/bin/bash

############################
#
# PRECONDITION: This process assumes that the previous date's (i.e. sytem time
#               minus 24 hours) autopush log file has already been processed by
#               the Heka aggregator and is available in the usual place in the
#               `heka-logs` S3 bucket.
#
############################
BASE_DIR=/opt/push_load
sudo yum install -y postgresql-libs
sudo yum install -y $BASE_DIR/rpms/luasandbox-0.10.2-Linux-core.rpm
sudo ldconfig
sudo yum install -y $BASE_DIR/rpms/hindsight-0.3.0-Linux.rpm

############################
#
# Before the following command will work, permissions need to be granted in two
# places. The first is ensuring the AWS S3 credentials are correctly configured
# to reach the `heka-logs` bucket. The second is ensuring the Redshift database
# settings are correct in the
# `$BASE_DIR/hindsight/hs_run/output/push_endp_redshift.cfg` file.
#
############################
hindsight_cli $BASE_DIR/hindsight/etc/hindsight.cfg

sudo pip install -r requirements.txt
sudo python setup.py develop
. db_creds.sh
ROLLUP_DB_STRING=postgresql+psycopg2://$DB_USER:$DB_PW@$DB_HOST:$DB_PORT/$DB_NAME
ROLLUP_TABLE_PREFIX=webpush_endpoint_requests
ROLLUP_DAYS_AGO=1
push_metrics process_logs

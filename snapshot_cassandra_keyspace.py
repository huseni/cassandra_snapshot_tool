#!/usr/bin/env python
#######################################################################################################################
#                                                                                                                     #
# THIS SCRIPT IS TO SNAPSHOT CASSANDRA KEYSPACES AND COLUMNS FROM EACH CLUSTER NODE                                   #
# VERSION 1.0                                                                                                         #
# USAGE:                                                                                                              #
#       python snapshot_cassandra_keyspace.py                                                                                  # 
#                                                                                                                     #
#######################################################################################################################
import sys
import subprocess
import os
from upload_cassandra_snapshot_to_s3 import AwsS3Wrapper

# Global variables
check_cassandra_process = "ps -ef | grep -v grep | grep cassandra | wc -l"
cassandra_process_id = 'ps -ef | grep -v grep | grep "cassandra" | awk \'{print $2}\''


def snapshot_cassandra_cluster(cmd):
    """
    To run the cassandra snapshot command on the node
    :return:
    """
    print("Attempting the start the cassandra snapshot..... ")
    rc = os.system(cmd)
    if rc is 0:
        print("cassandra snapshot has been run")
    else:
        print("ERROR: cassandra snapshot didn't run. please check")


def flush_cassandra_ssltable(cmd):
    """
    To run the SSTable flush on each node
    :return:
    """
    print("Attempting the start the cassandra snapshot..... ")
    rc = os.system(cmd)
    if rc is 0:
        print("cassandra sstable flush has been run")
    else:
        print("ERROR: cassandra sstable didn't run. please check")


def is_cassandra_running(process=None):
    """
    To Check if cassandra node process is running on the ec2 instance
    :param process:
    :return:
    """
    p_status = True
    cassandra_process = subprocess.Popen(process, stdout=subprocess.PIPE, shell=True)
    out, err = cassandra_process.communicate()
    if int(out) < 1:
        p_status = False
    return p_status


def cassandra_server_status(process_ids, process=None):
    """
    To Check the Cassandra cluster node process and take the actions to either start or shutdown the process
    :return:
    """
    if is_cassandra_running(process):
        t_pid = subprocess.Popen(process_ids, stdout=subprocess.PIPE, shell=True)
        out, err = t_pid.communicate()
        print("Cassandra process is running with PID " + out)
    else:
        print("Cassandra process is not running")


def main():
    global cassandra_process_id, check_cassandra_process
    nodetool = '/opt/apache-cassandra-1.2.14/bin/nodetool'
    snapshot_name = AwsS3Wrapper.get_snapshot_name()
    snapshot_cmd = '%s snapshot -t %s' % (nodetool, snapshot_name)
    sstable_flush_cmd = '%s flush' % nodetool

    # check the cassandra running status on the cluster node
    if not is_cassandra_running(check_cassandra_process):
        print("************* Cassandra process is not running on this node. Snapshot cannot be taken ************")
        sys.exit(1)
    print("******************* Cassandra process is up and running on the current cluster node ****************")

    print("************ Flushing Cassandra SSTable Started ****************")
    flush_cassandra_ssltable(sstable_flush_cmd)
    print("************ Flushing Cassandra SSTable Completed ****************")

    print("************ Snapshot Cassandra Started ******************")
    snapshot_cassandra_cluster(snapshot_cmd)
    print("************ Snapshot Cassandra Completed ******************")


if __name__ == '__main__':
    main()
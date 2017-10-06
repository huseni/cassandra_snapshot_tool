#!/usr/bin/env python
#######################################################################################################################
#                                                                                                                     #
# THIS SCRIPT IS TO CHECK THE CASSANDRA PROCESS STATUS, START OR STOP AS NEEDED.                                      #
# v 1.0                                                                                                               #
# USAGE:                                                                                                              #
#       refresh_cassandra_snapshot.py "status"                                                                        #
#       refresh_cassandra_snapshot.py "start"                                                                         #
#       refresh_cassandra_snapshot.py "stop"                                                                          #
#                                                                                                                     #
#######################################################################################################################
import subprocess
import time
import sys


# Global variables
filename = 'refresh_cassandra_snapshot.py'
sleep_time = 10
check_cassandra_process = "ps -ef | grep -v grep | grep cassandra | wc -l"
cassandra_process_id = 'ps -ef | grep -v grep | grep "cassandra" | awk \'{print $2}\''
start_cassandra_node = 'sudo service cassandra start'
stop_cassandra_node = 'sudo service cassandra stop'


# functions
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


def start_or_stop_cassandra_cluster(process=None, cmd=None):
    """
    To start or stop the cassandra server
    :return:
    """
    if cmd == "stop":
        if is_cassandra_running(process):
            print ("Stopping the cassandra node...")
            subprocess.Popen(stop_cassandra_node, stdout=subprocess.PIPE, shell=True)
            time.sleep(sleep_time)
            if is_cassandra_running(process):
                t_pid = subprocess.Popen([cassandra_process_id], stdout=subprocess.PIPE, shell=True)
                out, err = t_pid.communicate()
                subprocess.Popen(["kill -9 " + out], stdout=subprocess.PIPE, shell=True)
                print("Cassandra failed to shutdown, so killed with PID " + out)
        else:
            print("Cassandra server process is not running and shutdown")

    if cmd == "start":
        if is_cassandra_running(process):
            print("cassandra process is already running")
        else:
            print("Starting the cassandra node ...")
            cassandra_process = subprocess.Popen(start_cassandra_node, stdout=subprocess.PIPE, shell=True)
            out, err = cassandra_process.communicate()
            if int(out) > 1:
                print("cassandra process started successfully")
            else:
                print("Error while starting the cassandra node")


def script_usage():
    """
    To provide the details about how do you run this script
    :return:
    """
    print ("Usage: python " + filename + " start|stop|status")
    print "or"
    print ("Usage: <path>/" + filename + " start|stop|status")


def main():
    global check_cassandra_process, start_cassandra_node, stop_cassandra_node

    # Check for the number of commandline arguments. It must be '2'
    if len(sys.argv) != 2:
        print ("ERROR: Missing required arguments")
        script_usage()
        sys.exit(0)
    else:
        action = sys.argv[1]

    # Check the cassandra process status
    if action == 'status':
        cassandra_server_status(cassandra_process_id, check_cassandra_process)

    # start cassandra process
    if action == 'start':
        start_or_stop_cassandra_cluster(start_cassandra_node, 'start')

    # stop cassandra process
    if action == 'stop':
        start_or_stop_cassandra_cluster(stop_cassandra_node, 'stop')


# Main execution point
if __name__ == '__main__':
    main()
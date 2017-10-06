#!/usr/bin/env python
#######################################################################################################################
#                                                                                                                     #
# THIS SCRIPT IS TO UPLOAD THE CASSANDRA SNAPSHOT FILES FOR KEYSPACE SPECIFIED ONTO S3 BUCKET SPECIFIED               #
# VERSION 1.0                                                                                                         #
# USAGE:                                                                                                              #
#       upload_cassandra_snapshot_to_s3.py                                                                            #
#                                                                                                                     #
#######################################################################################################################
import boto3
from pprint import pprint
import os
import socket
from time import gmtime, strftime


class CassandraSnapshot(object):
    """
    This is to create multiple S3 objects and perfrom operations for upload files on s3 within the given s3 bucket
    """

    def __init__(self, s3_bucket_name, file_list=None):
        """
        To initialize the aws loadbalancer client to perform the operations on it.
        :param load_balancer_name:
        :param instance_id_list:
        """
        AWS_ACCESS_KEY_ID = '<>'
        AWS_SECRET_ACCESS_KEY = '<>'
        self.s3_client = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY_ID,
                                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')
        self.s3_client_api = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')
        self.s3_bucket_name = s3_bucket_name
        self.file_list = file_list

    def create_s3_bucket(self, acl, location_region):
        """
        To create new s3 bucket to store objects
        :return:
        """
        response = self.s3_client.create_bucket(ACL=acl, Bucket=self.s3_bucket_name, CreateBucketConfiguration={'LocationConstraint': location_region})
        pprint(response)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(" ******** AWS S3 bucket %s has been created ***********" % self.s3_bucket_name)
        else:
            raise IOError("Unable to create the s3 bucket on aws")
    def check_if_s3_bucket_exists(self):
        """
        To ensure s3
        :return:
        """
        s3_ducket_exists = False
        response = self.s3_client_api.head_bucket(Bucket=self.s3_bucket_name)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("AWS S3 bucket already exists. It will use the existing bucket.")
            s3_ducket_exists = True
            return s3_ducket_exists
        else:
            print("AWS S3 bucket %s does not exists" % self.s3_bucket_name)
            return s3_ducket_exists

    def upload_files_from_ec2_to_s3(self, file_list_dict):
        """
        To copy the files from aws instance to s3 bucket
        :return:
        """
        if not self.check_if_s3_bucket_exists():
            print("*************** Creating new S3 bucket...*****************")
            self.create_s3_bucket('public-read-write', location_region='ap-south-1')

        print("************************** Starting s3 uploads *********************************")
        for key, value in file_list_dict.items():
            for val in value:
                full_copy_file_path = os.path.join(key, val)
                try:
                    sub_dir = key[28:]
                    destination = os.path.join(sub_dir, val)
                    destination = CassandraSnapshot.get_cassandra_node_name() + '/' + destination
                    self.s3_client.meta.client.upload_file(full_copy_file_path, self.s3_bucket_name, destination)
                except IOError as e:
                    print("Error occurred while copying the file over s3", e)
                finally:
                    print("File %s has been uploaded onto s3 bucket %s successfully" % (full_copy_file_path, self.s3_bucket_name))

    @staticmethod
    def get_snapshot_file_list(filelist_path):
        """
        To return the filelist from the snapshot directory
        :return:
        """
        return os.listdir(filelist_path)

    @staticmethod
    def get_cassandra_node_name():
        """
        To get the cassandra cluster node name
        :return:
        """
        return socket.gethostname().split('.')[0]

    @staticmethod
    def get_snapshot_name():
        """
        To prepare the snapshot name, write into the file and return it
        :return:
        """
        current_date_time = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
        snapshot_date_time_name = """snapshot_%s_%s""" % (CassandraSnapshot.get_cassandra_node_name(), current_date_time)
        with open("snapshot_name", "w") as text_file:
            text_file.write("{0}".format(snapshot_date_time_name))
        return snapshot_date_time_name

    @staticmethod
    def read_snapshot_name_from_file():
        """
        To read the snapshot name from the file and return it
        :return:
        """
        with open("snapshot_name") as f:
            for line in f:
                if not line:
                    print("No snapshot name found. please check")
                    break
                cassandra_snapshot_name = line.strip()
        return cassandra_snapshot_name


def main():
    """
    Main Program execution point
    :return:
    """
    s3_bucket_name = "prod-cassandra-backup-bucket"  # Need to be provided by the user
    base_cassandra_path = 'ssd/var/lib/cassandra/data/my_prod_keyspace' # path where the backup of keyspace is sitting on the classandra node
    cassandra_snap = CassandraSnapshot(s3_bucket_name)
    snapshot_name = cassandra_snap.read_snapshot_name_from_file()

    snapshot_dir_filelist_dict = {
               '/%s/simple_store/snapshots/%s' % (base_cassandra_path, snapshot_name): cassandra_snap.get_snapshot_file_list('/%s/simple_store/snapshots/%s' % (base_cassandra_path, snapshot_name)),
        }
    cassandra_snap.upload_files_from_ec2_to_s3(snapshot_dir_filelist_dict)
    os.remove("snapshot_name")

# Main execution point
if __name__ == '__main__':
    main()

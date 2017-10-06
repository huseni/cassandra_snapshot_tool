#!/usr/bin/env python
#######################################################################################################################
#                                                                                                                     #
# THIS SCRIPT IS TO LOOK FOR S3 BUCKET SPECIFIED IN THE MAIN FUNCTION AND IF FOUND, THEN DOWNLOAD THE CASSANDRA       #
# SNAPSHOT ONTO A LOCAL MACHINE WITHIN THE SPECIFIED KEYSPACE DIRECTORY STRUCTURE                                     #
# VERSION 1.0                                                                                                         #
# USAGE:                                                                                                              #
#       python download_cassandra_snapshot_from_s3_bucket.py                                                          #
#                                                                                                                     #
#######################################################################################################################
import boto3
from pprint import pprint
import os


class AwsS3API(object):
    """
    This is to create the multiple aws S3 instance(s) to manipulate the data download and upload process.
    """

    def __init__(self, s3_bucket_name):
        """
        To initialize the aws S3 client and resource object client to perform the operations on it.
        :param load_balancer_name:
        :param instance_id_list:
        """
        AWS_ACCESS_KEY_ID = '<AWS ACCESS KEY>'
        AWS_SECRET_ACCESS_KEY = '<AWS SECRET KEY>'
        
        # AWS region must be modified when deploying this script to run it for US, Singapore or Japen region respectively
        self.s3_client = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')
        self.s3_client_api = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')
        self.s3_bucket_name = s3_bucket_name
        

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
        To ensure s3 bucket already exists before download process kicks off
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


    def download_files_from_s3_to_ec2(self, destination_directory_path):
        """
        To copy the files from s3 bucket to ec2 instance for refreshing the data onto the target cassandra setup
        :return:
        """
        if not self.check_if_s3_bucket_exists():
            print("AWS S3 bucket does not exists...Download cannot be occurred")

        print("************************** Starting s3 downloads *********************************")
        s3_bucket_list = self.s3_client_api.list_objects(Bucket=self.s3_bucket_name)['Contents']
        for s3_key in s3_bucket_list:
            s3_object = s3_key['Key']
            destination = s3_object[39:]
            final_destination = os.path.join('destination_directory_path' , destination)  # the specified path needs be 
            if not os.path.exists(os.path.dirname(final_destination)):
                os.makedirs(os.path.dirname(final_destination))

            self.s3_client_api.download_file(self.s3_bucket_name, s3_object, final_destination)
            print("File %s has been downloaded from s3 bucket %s successfully" % (s3_object, self.s3_bucket_name))


def main():
    """
    Main Program execution point
    :return:
    """
    # Initializing the bucket bucket
    s3_bucket_name = "prod-cassandra-backup-bucket"

    # path on ec2 machine where you want to download the hierarchical cassandra snapshot for the keyspace
    destination_directory_path = '/home/ec2-user/'
    
    # Download the cassandra backuped up snapshots
    cassandra_snapshot = AwsS3API(s3_bucket_name)
    cassandra_snapshot.download_files_from_s3_to_ec2(destination_directory_path)


# Main execution point
if __name__ == '__main__':
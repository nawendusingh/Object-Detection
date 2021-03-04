#!/usr/bin/env python3

import urllib.request
from framework_code.wrappers.ec2api import AwsEC2Controller
from framework_code.wrappers.s3api import AwsS3Controller
from framework_code.wrappers.sqsapi import AwsSQSController
import os
import time
import boto3


class taskMaster:
    def __init__(self,debug=False):
        self.debug = debug
        self.s3_handle = AwsS3Controller()
        self.sqs_handle = AwsSQSController()
        self.ec2_handle = AwsEC2Controller()
        self.max_active = 4

    #Assign task based on running/stopped/idle instances
    def scale_up(self):

        while True:
            #Check input queue size
            Q_size = int(self.sqs_handle.check_queue_size(Qname="data-queue"))

            if(Q_size > 0):
                print("Queue has messages. Checking for instance availability")

                #Get number of available instances :
                max_available_instances = int(self.ec2_handle.get_instance_count(state="stopped"))

                #Calculate the instances to deploy
                #need_to_deploy = Q_size - max_available_instances - 1   #-1 for webinstance

                to_deploy = min(Q_size, max_available_instances)

                print(to_deploy)

                if(to_deploy > 0):
                    stopped_instances = self.ec2_handle.get_instance_list(state='stopped')
                    new_deployment = []
                    for inst in stopped_instances:
                        new_deployment.append(inst.id)

                    self.ec2_handle.start_k_instances(new_deployment[0:to_deploy]) #get the first among stopped instance
                else:
                    print("All systems busy")
            #Check again after 20 seconds
            time.sleep(10)



if __name__ == "__main__":
    obj = taskMaster()
    obj.scale_up()

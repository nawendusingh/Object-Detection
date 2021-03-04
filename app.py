import urllib.request
from framework_code.wrappers.s3api import AwsS3Controller
from framework_code.wrappers.sqsapi import AwsSQSController
from framework_code.wrappers.ec2api import AwsEC2Controller
import subprocess
import os
import time


class AppController:
    def __init__(self, debug=False):
        debug = debug
        self.s3_handle = AwsS3Controller()
        self.sqs_handle = AwsSQSController()
        self.ec2_handle = AwsEC2Controller()

        # Get own instance id
        p = subprocess.Popen(
            [
                "wget",
                "-q",
                "-O",
                "-",
                "http://169.254.169.254/latest/meta-data/instance-id",
            ],
            stdout=subprocess.PIPE,
        )
        p.wait()
        op, _ = p.communicate()
        self.my_instance_id = str(op).split("'")[1]

        print(" App instance initialized")

    def check_task(self):
        while True:
            print("My Id : ", self.my_instance_id)
            data_value = self.sqs_handle.receive_msg(
                queue_name="input-data", msg="data_input"
            )
            print("DATA VALUE:", data_value)
            if data_value != "":
                video_file = data_value + ".h264"
                print(data_value, video_file)
                found_file = self.s3_handle.read_from_bucket(
                    key=data_value, output_file="/home/nawendu/" + video_file
                )
                if found_file:
                    print("Preparing to run darknet on : ", found_file)
                    subprocess.call(["chmod", "0777", "/home/nawendu/" + video_file])
                    print("Access-permissions changed for : ", found_file)
                    # run "detector.sh" shell script
                    p1 = subprocess.Popen(
                        [
                            "sh",
                            "/home/ubuntu/detector.sh",
                            "/home/ubuntu/" + video_file,
                        ],
                        stdout=subprocess.PIPE,
                    )
                    p1.wait()
                    p2 = subprocess.Popen(
                        ["cat", "/home/ubuntu/darknet/result_label"],
                        stdout=subprocess.PIPE,
                    )
                    p2.wait()
                    dnet_output, _ = p2.communicate()
                    self.s3_handle.write_to_bucket(
                        key=video_file,
                        output_file="/home/ubuntu/darknet/result_label",
                        bucket_index=1,
                    )
                    # remove video and results from local ec2 instance
                    os.remove("/home/ubuntu/darknet/result_label")
                    os.remove("/home/ubuntu/" + video_file)

                    self.sqs_handle.send_msg(
                        queue_name="output-data",
                        msg="data_output",
                        value=video_file + " : " + str(dnet_output),
                    )
            else:
                print("I am Done. Bye Bye...")
                # self.ec2_handle.stop_instance(instance_id=self.my_instance_id)


if __name__ == "__main__":
    obj = AppController()
    obj.check_task()


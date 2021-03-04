from framework_code.wrappers.s3api import AwsS3Controller
from framework_code.wrappers.sqsapi import AwsSQSController
import os

# it will check the cloud_video_store folder for videos and upload them to cloud


class VideoController:
    def __init__(self, debug=False):
        self.debug = debug
        self.s3_handle = AwsS3Controller()
        self.sqs_handle = AwsSQSController()

    def store_video_to_s3(self):
        # use a relative file path or absolute??
        # need to change for pi
        input_folder_name = "/home/nawendu/Desktop/cloudProject/video_store_cloud/"
        processed_folder_name = (
            "/home/nawendu/Desktop/cloudProject/video_store_processed/"
        )
        # lists all files
        files = sorted(os.listdir(input_folder_name))
        for i in range(len(files)):
            key_name = files[i].split(".")[0]
            suffix = ".h264"
            input_file = input_folder_name + key_name + suffix
            self.s3_handle.write_to_bucket(
                key=key_name, output_file=input_file, bucket_index=0
            )
            self.sqs_handle.send_msg(
                queue_name="input-data", msg="data_input", value=key_name
            )
            # processed_file = processed_folder_name + key_name + suffix
            # os.rename(input_file, processed_file)


if __name__ == "__main__":
    obj = VideoController()
    obj.store_video_to_s3()


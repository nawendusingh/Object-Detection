import boto3 as bt
from botocore.exceptions import ClientError
import time
import json


class AwsSQSController:
    def __init__(self, debug=True):

        self.debug = debug

        print("Initialized SQS Controller")
        self.sqs_client = bt.client("sqs")
        self.sqs = bt.resource("sqs")
        self.sqs_queue_names = ["input-data", "output-data"]
        self.prefix = "cloud-project1-team-any-"

    def create_queue(self, queue_name="all", prefix=""):
        def create(q_name):
            response = self.sqs_client.create_queue(
                QueueName=q_name,
                Attributes={
                    "DelaySeconds": "1",
                    "MessageRetentionPeriod": "86400",
                    "VisibilityTimeout": "20",
                },
            )
            if self.debug:
                print(response)

        if queue_name == "all":
            for q in self.sqs_queue_names:
                if self.debug:
                    print("creating queue ", q)
                create(prefix + q)
        else:
            if self.debug:
                print("creating queue : ", queue_name)
            create(queue_name)

    def delete_queue(self, queue_name="all", prefix=""):
        # You must wait 60 seconds after deleting a queue before you can create another with the same name.
        def delete(q_name):
            response_url = self.sqs_client.get_queue_url(QueueName=q_name)
            if self.debug:
                print(response_url["QueueUrl"])

            response = self.sqs_client.delete_queue(QueueUrl=response_url["QueueUrl"])
            if self.debug:
                print(response)

        if queue_name == "all":
            for q in self.sqs_queue_names:
                if self.debug:
                    print("Deleting queue ", q)
                delete(prefix + q)
        else:
            if self.debug:
                print("Deleting queue : ", queue_name)
            delete(queue_name)

    def send_msg(self, queue_name, msg, value):
        queue_name = self.prefix + queue_name
        q_list = self.sqs_client.list_queues(QueueNamePrefix=queue_name)
        # print(q_list)
        # qlist has length 2 when the queue with that name exists i.e. url and metadata
        # it has length 1 when queue does not exist
        if len(q_list) == 1:
            self.create_queue(queue_name=queue_name)
        url_response = self.sqs_client.get_queue_url(QueueName=queue_name)
        # url_response is a dictionary and we can extract the url of the queue
        q_url = url_response["QueueUrl"]
        if self.debug:
            print(url_response)
        # print(q_url)
        response = self.sqs_client.send_message(
            QueueUrl=q_url,
            MessageAttributes={msg: {"DataType": "String", "StringValue": msg}},
            MessageBody=value,
        )

        if self.debug:
            print("Message sent \n :", response)

    def receive_msg(self, queue_name, msg, max_msgs=1):
        print("\nFetching the message from queue\n")
        queue_name = self.prefix + queue_name
        q_list = self.sqs_client.list_queues(QueueNamePrefix=queue_name)
        print(q_list, len(q_list))
        if len(q_list) == 1:
            self.create_queue(queue_name=queue_name)
        url_response = self.sqs_client.get_queue_url(QueueName=queue_name)
        q_url = url_response["QueueUrl"]
        print(q_url)
        if self.debug:
            print(url_response)
        attempts = 0
        response = []
        print(len(response))
        # Keep polling
        while len(response) < 2:

            response = self.sqs_client.receive_message(
                QueueUrl=q_url,
                MessageAttributeNames=[msg],
                MaxNumberOfMessages=max_msgs,
                WaitTimeSeconds=20,
                VisibilityTimeout=20,
            )
            if self.debug:
                print(response)
                print("Length of  response : ", len(response))

            if attempts == 1:
                return ""

            attempts += 1
            if self.debug:
                print("Number of  attempts for message :", attempts)

        return self.read_message(response, q_url)

    def check_queue_size(self, queue_name):
        # returns number of messages available in the queue
        queue_name = self.prefix + queue_name
        q_list = self.sqs_client.list_queues(QueueNamePrefix=queue_name)
        if len(q_list) == 1:
            self.create_queue(queue_name=queue_name)
        url_response = self.sqs_client.get_queue_url(QueueName=queue_name)
        q_url = url_response["QueueUrl"]
        if self.debug:
            print(url_response)
        response = self.sqs_client.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["ApproximateNumberOfMessages"],
        )
        if self.debug:
            print(response["Attributes"]["ApproximateNumberOfMessages"])
        return response["Attributes"]["ApproximateNumberOfMessages"]

    def output_msg(self, queue_name, msg, max_msgs=1):
        # for sending message from input-queue to output-queue. no need to pool
        queue_name = self.prefix + queue_name
        q_list = self.sqs_client.list_queues(QueueNamePrefix=queue_name)
        if len(q_list) == 1:
            self.create_queue(queue_name=queue_name)
        url_response = self.sqs_client.get_queue_url(QueueName=queue_name)
        q_url = url_response["QueueUrl"]
        if self.debug:
            print(url_response)
        response = self.sqs_client.receive_message(
            QueueUrl=q_url,
            MessageAttributeNames=[msg],
            MaxNumberOfMessages=max_msgs,
            WaitTimeSeconds=20,
            VisibilityTimeout=20,
        )
        if len(response) < 2:
            return ""

        return self.read_message(response, q_url)

    def read_message(self, response, q_url):
        if self.debug:
            print("Reading the message: ")
        full_value = ""
        for i in range(len(response["Messages"])):
            message = response["Messages"][i]
            value = message["Body"]
            receipt_handle = message["ReceiptHandle"]
        if self.debug:
            print("Received message")
            print("message = \n", value)
            print("handle = \n  ", receipt_handle)
            # # Delete received message from queue
            if self.debug:
                print("Deleting message and returning data..")
            self.sqs_client.delete_message(QueueUrl=q_url, ReceiptHandle=receipt_handle)

            if len(response["Messages"]) == 1:
                full_value += value
            else:
                full_value += value + ";"
        # x = full_value
        # print(x)
        return full_value


if __name__ == "__main__":
    obj = AwsSQSController(debug=True)
    # obj.create_queue(prefix=obj.prefix)
    # obj.delete_queue(prefix=obj.prefix)
    # obj.send_msg("input-data", "test-msg", "Hello-World")
    # obj.send_msg("input-data", "test-msg-2", "Hello-World-2")
    # obj.send_msg("input-data", "test-msg-3", "Hello-World-3")
    # obj.send_msg("input-data", "test-msg-4", "Hello-World-4")
    # obj.send_msg("input-data", "test-msg-5", "Hello-World-5")
    # obj.send_msg("input-data", "test-msg-6", "Hello-World-6")
    # obj.send_msg("input-data", "test-msg-7", "Hello-World-7")
    # # time.sleep(20)
    message = obj.receive_msg("input-data", "test-msg")
    message2 = obj.receive_msg("input-data", "test-msg2")
    message3 = obj.receive_msg("input-data", "test-msg3")
    message4 = obj.receive_msg("input-data", "test-msg4")
    message5 = obj.receive_msg("input-data", "test-msg5")
    message6 = obj.receive_msg("input-data", "test-msg6")
    message7 = obj.receive_msg("input-data", "test-msg7")

    # print(("1", message))
    # # # time.sleep(30)
    # message2 = obj.output_msg("input-data", "test-msg-2")
    # time.sleep(30)
    # print(("2", message2))

    # obj.check_queue_size("data-info")


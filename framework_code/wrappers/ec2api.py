import boto3 as bt
from botocore.exceptions import ClientError


class AwsEC2Controller:
    def __init__(self):
        print("Aws EC2 Controller Initialized")
        self.ec2 = bt.resource("ec2")
        self.ec2_client = bt.client("ec2")
        self.count_all_instances = 19
        # saving one instance for AWS ban
        self.count_active = 0  # All instances are shutdown initially
        # self.count_inactive = 3

    def create_instances(self):
        pass

    # We will create instances manually and keep it in stopped state, will save us time
    # --------------------------------#
    #           Start Instance        #
    # --------------------------------#
    def start_one_instance(self, instance_id):
        print("starting instance : ", instance_id)
        ec2_waiter = self.ec2_client.get_waiter("instance_running")
        try:
            #   Do a dryrun first to verify permissions
            self.ec2_client.start_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if "DryRunOperation" not in str(e):
                raise
        # Dry run succeeded, run start_instances without dryrun
        try:
            response = self.ec2_client.start_instances(
                InstanceIds=[instance_id], DryRun=False
            )
            ec2_waiter.wait(InstanceIds=[instance_id])
        except ClientError as e:
            print(e)

    # -------------------------#
    # start multiple instances #
    # -------------------------#
    def start_n_instances(self, instance_list):
        ec2_waiter = self.ec2_client.get_waiter("instance_running")
        responses = self.ec2_client.start_instances(
            InstanceIds=instance_list, DryRun=False
        )
        ec2_waiter.wait(InstanceIds=instance_list)

    def stop_instance(self, instance_id):
        print("Stopping instance", instance_id)
        ec2_waiter = self.ec2_client.get_waiter("instance_stopped")
        try:
            self.ec2_client.stop_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if "DryRunOperation" not in str(e):
                raise
            # Dry run succeeded, call stop_instances without dryrun
        try:
            response = self.ec2_client.stop_instances(
                InstanceIds=[instance_id], DryRun=False
            )
        except ClientError as e:
            print(e)

    def list_instance(self, state="stopped"):
        # Get list of all instances
        # To get inactive instances, change default parameter 'running' to 'stopped'
        state_instances = self.ec2.instances.filter(
            Filters=[{"Name": "instance-state-name", "Values": [state]}]
        )
        state_instances_size = sum(1 for _ in state_instances)
        # returns number of instances in requested state
        print(state, " : ", state_instances_size)

        for instance in state_instances:
            print(instance.id, instance.instance_type)

        # return list(state_instances)

    def detail_instance(self):
        # Print all  instances
        print("Instance Ids : ")
        # we can get the instance id for stopping /spinning up instances
        for instance in self.ec2.instances.all():
            print(instance)

        # print("Details : ")
        response = self.ec2_client.describe_instances()
        # print(response)


if __name__ == "__main__":
    obj = AwsEC2Controller()
    # obj.start_instance("i-07df79accca04995b")
    # obj.stop_instance("i-015e9566b4b6dee44")
    # obj.detail_instance()
    # obj.list_instance()


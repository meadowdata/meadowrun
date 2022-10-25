# AWS resources created by Meadowrun

This page lists all AWS resources created by using meadowrun or running
`meadowrun-manage-ec2 install`. All of these resources can be deleted automatically by
running `meadowrun-manage-ec2 uninstall`.

* EC2
    * Instances: tagged with "meadowrun = TRUE"
    * SSH Key Pair: meadowrun_key_pair
    * Security Group: meadowrun_ssh_security_group
* Secrets
    * meadowrun_private_key
* SQS
    * Queues: names start with "meadowrun-task"
* Lambdas
    * meadowrun_ec2_alloc_lambda
    * meadowrun_clean_up
* EventBridge
    * meadowrun_ec2_alloc_lambda_schedule_rule
    * meadowrun_clean_up_lambda_schedule_rule
* Logs
    * Lambdas will automatically generate logs under /aws/lambda/<lambda name>
* DynamoDB:
    * meadowrun_ec2_alloc_table
* ECR:
    * meadowrun_generated
* IAM
    * Role: meadowrun_ec2_role, associated instance profile (meadowrun_ec2_role_instance_profile), and associated policy (meadowrun_ec2_policy)
    * Role: meadowrun_management_lambda_role and associated policy (meadowrun_management_lambda_policy)
    * User group: meadowrun_user_group and associated policy (meadowrun_user_policy)
* S3
    * Buckets: meadowrun-<region>-<account number> with 14 day object expiry policy
    * Policy: meadowrun_s3_access
# AWS resources created by meadowrun

This page lists all AWS resources created by using meadowrun or running
`meadowrun-manage-ec2 install`. All of these resources can be deleted automatically by
running `meadowrun-manage-ec2 uninstall`.

* EC2
    * Instances: tagged with "meadowrun_ec2_alloc = true"
    * SSH Key Pair: meadowrunKeyPair
    * Security Group: meadowrunSshSecurityGroup
* Secrets
    * meadowrunKeyPairPrivateKey
* SQS
    * Queues: names start with "meadowrunTask"
* Lambdas
    * meadowrun_ec2_alloc_lambda
    * meadowrun_clean_up
* EventBridge
    * meadowrun_ec2_alloc_lambda_schedule_rule
    * meadowrun_clean_up_lambda_schedule_rule
* Logs
    * Lambdas will automatically generate logs under /aws/lambda/<lambda name>
* DynamoDB:
    * _meadowrun_ec2_alloc_table
* ECR:
    * meadowrun_generated
* IAM
    * Role: meadowrun_ec2_alloc_role and associated instance profile
    * Role: meadowrun_management_lambda_role
    * Policy: meadowrun_ec2_alloc_table_access
    * Policy: meadowrun_sqs_access
    * Policy: meadowrun_ecr_access
* S3
    * Buckets: meadowrun-<region>-<uuid> with 14 day object expiry policy
    * Policy: meadowrun_s3_access
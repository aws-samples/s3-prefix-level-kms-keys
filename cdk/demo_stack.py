#!/usr/bin/env python3

import aws_cdk as cdk
from constructs import Construct
from aws_cdk import (
    aws_kms as kms,
    aws_iam as iam,
    aws_s3 as s3,
    custom_resources as custom_resources,
    CfnOutput,
    Stack
)
from cdk_nag import NagSuppressions

num_prefixes = 3

#This class is only for demo purposes. This class represents a stack that has a bucket and a few KMS keys.
#You can use either versioned or non-versioned buckets [see the caveat when using versioned buckets in the README file]
#This stack contains the following resources:
#1. S3 Bucket
#2. KMS Keys

class DemoResourcesStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket_versioned = False, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        ############################################
        #                  Bucket                  #
        ############################################

        #Since this is a demo bucket, setting up a life cycle rule to expire objects after 30 days.
        lifecycle_rule_mpu = s3.LifecycleRule(
            abort_incomplete_multipart_upload_after=cdk.Duration.days(30),
            enabled=True)
        lifecycle_rule_deletion = s3.LifecycleRule(
            expiration=cdk.Duration.days(30),
            noncurrent_version_expiration=cdk.Duration.days(30),
            enabled=True
        )

        #Create an S3 bucket for demo purposes
        self.demo_bucket = s3.Bucket(
                        self,
                        "prefix-level-keys-demo-bucket",
                        removal_policy=cdk.RemovalPolicy.DESTROY,
                        public_read_access=False,
                        encryption=s3.BucketEncryption.S3_MANAGED,
                        auto_delete_objects = True, #This ensures that objects are deleted automatically when the bucket is destroyed as part of the stack.
                        versioned=bucket_versioned, #If versioned is set to True, then the bucket is versioned.This solution could lead to unexpected results for versioned buckets and hence not advisable. Please refer to the "Caveats" section of the README file
                        enforce_ssl=True, #This ensures that the bucket is only accessible via HTTPS. This is a good practice.
                        lifecycle_rules=[lifecycle_rule_mpu, lifecycle_rule_deletion] #Add the lifecycle rules to the bucket. This will ensure that objects are deleted after 30 days and that incomplete MPUs are cleand up.
                    )
        CfnOutput(self, "S3BucketName", value=self.demo_bucket.bucket_name)

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            '/'+self.stack_name+'/prefix-level-keys-demo-bucket/Resource',
            [
                {
                'id': 'AwsSolutions-S1',
                'reason': 'This is only a demo bucket where nothing sensitive is being stored. Hence access logs are not enabled.',
                },
            ]
        )
        #---------------End of Bucket---------------

        ############################################
        #                 KMS Keys                 #
        ############################################
        #Create a policy to use the KMS keys
        self.key_usage_policy = iam.PolicyDocument(
            statements=
                [
                    iam.PolicyStatement(
                        sid = "AllowIAMRolesToManageThisKey",
                        actions=["kms:*"],
                        principals=[iam.AccountRootPrincipal()],
                        resources=["*"]
                        ), #The use of "*" does not violate the principle of Least Privilege. This allows the account to use IAM to manage access to this key. Given so that the access to these keys are not locked out. Ref: https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-overview.html#key-policy-example
                ]
        )


        #KMS Keys for different Prefixes
        self.encryption_keys = []
        #Creating 10 different keys for demo purposes.
        for i in range(num_prefixes):
            encryption_key = kms.Key(self, f"Key_for_prefix{i+1}",
                enable_key_rotation=True,
                key_usage = kms.KeyUsage.ENCRYPT_DECRYPT,
                policy = self.key_usage_policy,
                #alias = f"Key_for_prefix{i+1}",
            )
            self.encryption_keys.append(encryption_key)
            #Add the key to the outputs section.
            CfnOutput(self, f"Output_Key_for_prefix{i+1}", value = encryption_key.key_arn)

            #Get KMS key's L1 construct and add a meta data explaining the use of "*" in the key usage policy
            encryption_key_l1 = encryption_key.node.default_child
            encryption_key_l1.add_metadata("Comment", "The use of '*' does not violate the principle of Least Privilege. This allows the account to use IAM to manage access to this key. Given so that the access to these keys are not locked out. Ref: https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-overview.html#key-policy-example")



        #---------------End of KMS Keys---------------

#!/usr/bin/env python3

import json
from constructs import Construct
from datetime import datetime
from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
    custom_resources as custom_resources,
    aws_s3_notifications as s3n,
    NestedStack,
    RemovalPolicy,
    CustomResource,
    Annotations
)

from cdk_nag import NagSuppressions

#Each instance of this stack represents a single bucket whose prefix level keys are enforced by the core stack.
#This stack will create the following resources:
#1. A custom resource that updates DynamoDB mapping Table with the prefixes and KMS keys of this bucket
#2. A custom resource that adds inline policy to Lambda role to allow Lambda to write to the bucket and to encrypt/decrypt with the KMS keys for this bucket
#3. Adding event notification configuration to the bucket so that any objects uploaded to the bucket triggers a message to the SQS queue defined in the core stack.

class BucketIntegrationStack(NestedStack):
    def __init__(self, scope: Construct, id: str, bucket_name: str, mapping_data, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.bucket_name = bucket_name
        self.core_stack = scope

        ###################################################
        #   Custom Resource to initialize the DDB Table   #
        ###################################################

        self.ddb_init_custom_resource = CustomResource(
            scope=self,
            id=f'DDBMappingTableInitFor{bucket_name}',
            service_token=self.core_stack.ddb_init_provider.service_token,
            removal_policy=RemovalPolicy.DESTROY,
            resource_type="Custom::DDBMappingTableInit",
            properties={
                    "TableName" : self.core_stack.ddb_mapping_table.table_name,
                    "MappingData" : mapping_data,
                    "BucketName" : bucket_name
                    },
        )

        ##################################################################################################
        #   Custom Resource to add inline policy to Lambda role to allow Lambda to write to the bucket,  #
        #   and to encrypt decrypt keys for the prefixes in the bucket                                   #
        ##################################################################################################

        #Get IAM Role ARN from core_stack
        lambda_role_arn = self.core_stack.enforce_encryption_lambda_fn.role.role_arn
        lambda_role_name = self.core_stack.enforce_encryption_lambda_fn.role.role_name
        
        #Generate bucket ARN from bucket name
        bucket_arn = f"arn:aws:s3:::{self.bucket_name}"
        kms_key_arns = [mapping_data[prefix]['kms_key_arn'] for prefix in mapping_data]

        #Generate policy document for the Lambda role to allow Lambda to write to the bucket and encrypt decrypt keys for the prefixes in the bucket.
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:ListBucket",
                        "s3:DeleteObject",
                        "s3:DeleteObjectVersion"
                    ],
                    "Resource": [
                        bucket_arn,
                        f"{bucket_arn}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:GenerateDataKey",
                    ],
                    "Resource": kms_key_arns
                }
            ]
        }

        self.grant_lambda_s3_access_resource_call = custom_resources.AwsSdkCall(
            region = self.region,
            service = "IAM",
            action = "putRolePolicy",
            parameters = {'RoleName':lambda_role_name,
                          'PolicyName':f"permissions_for_bucket_{self.bucket_name}",
                          'PolicyDocument':json.dumps(policy_document)},
            physical_resource_id=custom_resources.PhysicalResourceId.of(datetime.now().strftime("%Y:%m:%d:%H:%M:%S:%f"))
        )

        self.revoke_lambda_s3_access_resource_call = custom_resources.AwsSdkCall(
            region = self.region,
            service = "IAM",
            action = "deleteRolePolicy",
            parameters = {'RoleName':lambda_role_name, 
                          'PolicyName':f"permissions_for_bucket_{self.bucket_name}"},
            physical_resource_id=custom_resources.PhysicalResourceId.of(datetime.now().strftime("%Y:%m:%d:%H:%M:%S:%f"))
        )

        self.grant_lambda_s3_access_resource = custom_resources.AwsCustomResource(
            self,
            "GrantLambdaS3AccessResource",
            on_create = self.grant_lambda_s3_access_resource_call,
            on_update = self.grant_lambda_s3_access_resource_call,
            #on_delete = self.revoke_lambda_s3_access_resource_call,
            policy=custom_resources.AwsCustomResourcePolicy.from_statements(
                [iam.PolicyStatement(actions = ["iam:putRolePolicy", "iam:deleteRolePolicy"], resources = [lambda_role_arn])]
            )
        )

        NagSuppressions.add_resource_suppressions_by_path(
            #self.grant_lambda_s3_access_resource,
            self,
            f'/S3PrefixLevelKeys/{id}/AWS679f53fac002430cb0da5b7982bd2287/Resource',
            suppressions = [
                {
                'id': 'AwsSolutions-L1',
                'reason': 'The custom resource generates the Lambda Function. But that is outside the scope of this code',
                }
            ]
        )

        NagSuppressions.add_resource_suppressions_by_path(
            #self.grant_lambda_s3_access_resource,
            self,
            f'/S3PrefixLevelKeys/{id}/AWS679f53fac002430cb0da5b7982bd2287/ServiceRole/Resource',
            suppressions = [
                {
                'id': 'AwsSolutions-IAM4',
                'reason': 'The custom resource generates the IAM role for the Lambda function. But that is outside the scope of this code',
                }
            ]
        )

        ###############################################################################################
        # Add event notification for S3 object creation pointing to the SQS queue in the core stack   #
        ###############################################################################################

        s3_bucket = s3.Bucket.from_bucket_name(self, "S3Bucket", self.bucket_name)

        #Add event notification to the bucket so that events are sent to the SQS queue created in the core stack
        s3_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SqsDestination(self.core_stack.s3_objects_queue))

        NagSuppressions.add_resource_suppressions_by_path(
            #self.grant_lambda_s3_access_resource,
            self,
            f'/S3PrefixLevelKeys/{id}/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/Resource',
            suppressions = [
                {
                'id': 'AwsSolutions-IAM4',
                'reason': 'The add_event_notification function generates the Lambda function and the corresponding IAM Role. But that is outside the scope of this code',
                }
            ]
        )

        NagSuppressions.add_resource_suppressions_by_path(
            #self.grant_lambda_s3_access_resource,
            self,
            f'/S3PrefixLevelKeys/{id}/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/DefaultPolicy/Resource',
            suppressions = [
                {
                'id': 'AwsSolutions-IAM5',
                'reason': "The add_event_notification function generates the Lambda function and the default policy for the function's IAM Role. But that is outside the scope of this code",
                }
            ]
        )

        #------------End of Event Notification ------------

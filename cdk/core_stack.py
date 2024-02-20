#!/usr/bin/env python3
import os
import re

import json
from constructs import Construct
from aws_cdk import (
    aws_iam as iam,
    aws_kms as kms,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
    custom_resources as custom_resources,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    Annotations
)
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from integration_stack import BucketIntegrationStack
from aws_cdk.custom_resources import Provider
from aws_cdk.aws_lambda import AdotLambdaExecWrapper, AdotLayerVersion, AdotLambdaLayerPythonSdkVersion

from cdk_nag import NagSuppressions


#This stack is the core stack that contains the necessary resources to enforce prefix level KMS keys.
#There will only be one instance of this stack no matter how many buckets are being covered.
#This stack will create the following resources:
#1. DynamoDB Table that stores the mapping between prefixes and KMS keys
#2. DynamoDB Table that stores the logs of the actions of the Lambda function
#3. SQS Queue to which S3 object notifications will be sent
#4. Lambda function that will receive messages from SQS queue and enforces the KMS keys
#5. SNS Topic to which the Lambda function will send send notifications in case of failures
#6. SNS Subscription that will be used to receive notifications from the SNS Topic
#7. A Lambda function that will keep the mapping table upto date with the input files referred to in the "input_file_name" context variable
#8. One Integration Stack for each bucket that needs to be covered by this stack.
class CoreResourcesStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        #Validate all the context variables
        self.validate_context_variables()

        #This input file provides a list of bucket names and the json file that contains the prefix->KMS Key mapping for that bucket
        self.input_file_name = self.node.try_get_context("input_file_name")

        ############################################
        #                DDB Tables                #
        ############################################
        #Create a DynamoDB Table that stores the mapping between prefixes and KMS keys
        self.ddb_mapping_table = dynamodb.Table(
            self, "PrefixLevelKeysMappingTable",
            partition_key = dynamodb.Attribute(name = "bucket_name", type = dynamodb.AttributeType.STRING),
            sort_key = dynamodb.Attribute(name = "prefix", type = dynamodb.AttributeType.STRING),
            removal_policy = RemovalPolicy.DESTROY,
            billing_mode = dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True
        )

        retain_ddb_logs_table = self.node.try_get_context("retain_ddb_logs_table")
        if retain_ddb_logs_table and (retain_ddb_logs_table == True or retain_ddb_logs_table.lower() == 'true'): #We need to check for both string and boolean values because while boolean values can be passed in via cdk.json, it cannot be passed in at the command line. All parameters passed in from the command line are received as strings
            Annotations.of(self).add_info("DDB Logs table will be retained even after stack deletion")
            logs_removal_policy = RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE
        else:
            Annotations.of(self).add_info("DDB Logs table will be deleted on stack deletion. If you want to retain it, please pass in the context variable retain_ddb_logs_table as True")
            logs_removal_policy = RemovalPolicy.DESTROY
        #Create a DynamoDB table that logs the changes made
        self.ddb_log_table = dynamodb.Table(
            self, "PrefixLevelKeysLogTable",
            partition_key = dynamodb.Attribute(name = "s3_object_path", type = dynamodb.AttributeType.STRING),
            sort_key = dynamodb.Attribute(name = "current_timestamp_utc", type = dynamodb.AttributeType.STRING),
            removal_policy = logs_removal_policy,
            billing_mode = dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True
        )

        #-------------End of DDB Table--------------

        ####################################################
        #            Enforce Encryption Lambda             #
        ####################################################
        #Create a Lambda function to Enforce the right key on the S3 objects.
        #The Lambda function will be triggered when a new S3 object is added to the bucket.

        # But before creating the function, first create the role.
        # We are not using the default role as it uses AWS Managed Policy "AWSLambdaBasicExecutionRole"
        # and the resources are not narrowed down
        self.enforce_encryption_lambda_role = iam.Role(self, "EnforceEncryptionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))

        self.enforce_encryption_lambda_fn = lambda_.Function(self, "EnforcePrefixLevelEncryption",
                runtime=lambda_.Runtime.PYTHON_3_12,
                handler="s3_encrypt.lambda_handler",
                code=lambda_.Code.from_asset("resources/fix_encryption"),
                environment = {
                    "ddb_mapping_table" : self.ddb_mapping_table.table_name,
                    "ddb_log_table" : self.ddb_log_table.table_name
                    },
                adot_instrumentation=lambda_.AdotInstrumentationConfig(
                    layer_version=AdotLayerVersion.from_python_sdk_layer_version(AdotLambdaLayerPythonSdkVersion.LATEST),
                    exec_wrapper=AdotLambdaExecWrapper.INSTRUMENT_HANDLER
                ),
                timeout = Duration.seconds(60*15),
                role = self.enforce_encryption_lambda_role
            )

        #Build log group and log stream ARNs
        log_group_arn = f"arn:{self.partition}:logs:{self.region}:{self.account}:log-group:/aws/lambda/{self.stack_name}-EnforcePrefixLevelEncryption*"
        log_stream_arn = f"arn:{self.partition}:logs:{self.region}:{self.account}:log-group:/aws/lambda/{self.stack_name}-EnforcePrefixLevelEncryption*:log-stream:*"

        #Add a policy to the lambda execution role to be able to send logs to CloudWatch
        self.enforce_encryption_lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources = [log_group_arn, log_stream_arn]
            )
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            '/S3PrefixLevelKeys/EnforceEncryptionLambdaRole/DefaultPolicy/Resource',
            [
                {
                'id': 'AwsSolutions-IAM5',
                'reason': 'The IAM policy uses "*" for telemetry events but this is added by default by the ADOTLayer in CDK. * is also used in log-groups and log-strreams as it is not posible to know the full name before the stack synthesizes',
                },
            ]
        )

        #########################################################
        #        DDB Mapping Table Initialization Lambda        #
        #########################################################
        #Create a Lambda function for handling Mapping tables updates
        # But before creating the function, first create the role.
        # We are not using the default role as it uses AWS Managed Policy "AWSLambdaBasicExecutionRole"
        # and the resources are not narrowed down
        self.ddb_init_lambda_fn_role = iam.Role(self, "MappingTableInitializationLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))

        self.ddb_init_lambda_fn = lambda_.Function(self, "MappingTableInitialization",
                runtime=lambda_.Runtime.PYTHON_3_12,
                handler="ddb_custom_resource.on_event",
                code=lambda_.Code.from_asset("resources/custom_resources"),
                adot_instrumentation=lambda_.AdotInstrumentationConfig(
                    layer_version=AdotLayerVersion.from_python_sdk_layer_version(AdotLambdaLayerPythonSdkVersion.LATEST),
                    exec_wrapper=AdotLambdaExecWrapper.INSTRUMENT_HANDLER
                ),
                timeout = Duration.seconds(60),
                role = self.ddb_init_lambda_fn_role
            )

        #Build log group and log stream ARNs
        log_group_arn = f"arn:{self.partition}:logs:{self.region}:{self.account}:log-group:/aws/lambda/{self.stack_name}-MappingTableInitialization*"
        log_stream_arn = f"arn:{self.partition}:logs:{self.region}:{self.account}:log-group:/aws/lambda/{self.stack_name}-MappingTableInitialization*:log-stream:*"

        #Add a policy to the lambda execution role to be able to send logs to CloudWatch
        self.ddb_init_lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                #Add specific log groups and log streams - yet to debug why specific log groups do not work TBD
                resources = [log_group_arn, log_stream_arn]
            )
        )

        #Grant the Lambda function permissions to query, update, delete items from the DDB table.
        self.ddb_init_lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["dynamodb:Query", "dynamoDB:BatchWriteItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"],
                resources = [self.ddb_mapping_table.table_arn]
            )
        )

        self.ddb_init_provider = Provider(scope=self, 
                            id=f'DDBMappingInitProvider', 
                            on_event_handler=self.ddb_init_lambda_fn)

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            '/S3PrefixLevelKeys/MappingTableInitializationLambdaRole/DefaultPolicy/Resource',
            [
                {
                'id': 'AwsSolutions-IAM5',
                'reason': 'The IAM policy uses "*" for telemetry events but this is added by default by the ADOTLayer in CDK. * is also used in log-groups and log-strreams as it is not posible to know the full name before the stack synthesizes',
                },
            ]
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            '/S3PrefixLevelKeys/DDBMappingInitProvider/framework-onEvent/ServiceRole/Resource',
            [
                {
                'id': 'AwsSolutions-IAM4',
                'reason': 'The Managed policy administered by AWS does not restrict scope, but it is used by the CDK construct and so outide the scope of this code',
                },
            ]
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            '/S3PrefixLevelKeys/DDBMappingInitProvider/framework-onEvent/ServiceRole/DefaultPolicy/Resource',
            [
                {
                'id': 'AwsSolutions-IAM5',
                'reason': 'The Managed policy administered by AWS does not restrict scope and uses *, but it is used by the CDK construct and so outide the scope of this code',
                },
            ]
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            '/S3PrefixLevelKeys/DDBMappingInitProvider/framework-onEvent/Resource',
            [
                {
                'id': 'AwsSolutions-L1',
                'reason': 'The provider is defined in the CDK and it does not use python 3.12. But that is outside the scope of this code',
                },
            ]
        )


        #---------------End of Lambda---------------

        ############################################
        #               SQS queues                 #
        ############################################
        #Create a dead letter queue for the main SQS queue
        self.dead_letter_queue = sqs.Queue(self, "S3ObjectsDLQ",
                                            encryption = sqs.QueueEncryption.SQS_MANAGED,
                                            enforce_ssl = True)

        #Create a dead letter queue construct to specify the max_receive_count
        max_receive_count = self.node.try_get_context("max_receive_count")
        if not max_receive_count:
            max_receive_count = 1
        else:
            max_receive_count = int(max_receive_count)
        
        self.dead_letter_queue_settings = sqs.DeadLetterQueue(max_receive_count=max_receive_count,
                                                     queue = self.dead_letter_queue)

        #Create an SQS queue for the S3 objects that get added to the bucket.
        #Messages will be added to the queue from S3 object notificaitons
        self.s3_objects_queue = sqs.Queue(self, "S3ObjectsQueue",
                                    visibility_timeout=Duration.seconds(15*60), #This must be at least as much as the lambda function time out.
                                    dead_letter_queue = self.dead_letter_queue_settings,
                                    encryption = sqs.QueueEncryption.SQS_MANAGED,
                                    enforce_ssl = True
                                    )

        #---------------End of SQS---------------

        ##############################################
        #            Error Notification              #
        ##############################################

        #Create a cloudwatch alarm to notify an SNS topic if the DLQ has any messages
        self.dead_letter_queue_alarm = cloudwatch.Alarm(
            self, "DeadLetterQueueAlarm",
            metric = self.dead_letter_queue.metric_approximate_number_of_messages_visible(),
            threshold = 1, 
            evaluation_periods = 1,
            comparison_operator = cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        )

        #Create an SNS topic to send notifications to
        self.sns_topic = sns.Topic(self,
                                   "DeadLetterQueueNotificationTopic",
                                   master_key = kms.Alias.from_alias_name(self, id = "AWSManagedDefaultSNSKeyAlias", alias_name = "alias/aws/sns")
                                )

        self.sns_topic.grant_publish(iam.ServicePrincipal("cloudwatch.amazonaws.com"))
        error_email_address = self.node.try_get_context("error_notification_email")
        if error_email_address:
            self.sns_topic.add_subscription(sns_subscriptions.EmailSubscription(error_email_address))

        #Add an Alarm action to notify the SNS topic when the alarm is fired
        self.dead_letter_queue_alarm.add_alarm_action(cloudwatch_actions.SnsAction(self.sns_topic))
        #---------------End of SQS---------------


        #################################################
        #               Permissions setup               #
        #################################################
        #Grant the Lambda function permissions to query the DDB Mapping table.
        self.enforce_encryption_lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["dynamodb:Query"],
                resources = [self.ddb_mapping_table.table_arn]
            )
        )

        #Grant the Lambda function permissions to insert data into the DDB Logs table.
        self.enforce_encryption_lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions = ["dynamodb:PutItem"],
                resources = [self.ddb_log_table.table_arn]
            )
        )

        #Create an event source from SQS to Enforcement Lambda function
        event_source = SqsEventSource(self.s3_objects_queue,
                                        batch_size=10,
                                        max_batching_window=Duration.seconds(15),
                                        report_batch_item_failures=True)

        #Grant the SQS queue permissions to send messages to the Lambda function. Needed for event notifications to work.
        self.s3_objects_queue.grant_send_messages(self.enforce_encryption_lambda_fn)

        #Set SQS queue as event source for lambda function
        self.enforce_encryption_lambda_fn.add_event_source(event_source)

        #Allow the S3 service to send messages to SQS queue
        self.s3_objects_queue.grant_send_messages(iam.ServicePrincipal("s3.amazonaws.com"))
        #-------------End of Permissions--------------

        #################################################
        #               S3 Buckets setup                #
        #################################################
        # These buckets are not part of this stack. These are buckets that need to have prefix level keys enforced
        # and hence we will do the necessary permissions set up on the buckets and the KMS keys so that this app can enforce the keys.
        # Each bucket that is protected by this app will have its own instance of the "Integration" stack. 
        # That way, bucket level configuration is separated out and if we decide to remove protection for the bucket, then that nested stack alone can be deleted.
        # This section needs to be executed only if there is an input file that contains the bucket and prefix names mapping to KMS keys
        if self.input_file_name:
            input_data = self.read_json_file(self.input_file_name)
            self.bucket_integration_stacks = []

            #Create an "Integration" stack for each bucket that is protected by this app.
            for bucket_name in input_data:
                #Read the mapping data
                mapping_data = self.read_json_file(input_data[bucket_name]["mapping_file_name"])
                #Instantiate the stack
                self.bucket_integration_stacks.append(BucketIntegrationStack(self, f"BucketIntegrationStack_{bucket_name}", bucket_name=bucket_name, mapping_data = mapping_data))
        #-------------End of Bucket Level setup-----------

        ###################################################
        #                     Outputs                     #
        ###################################################
        CfnOutput(self, "DDBMappingTableName", value=self.ddb_mapping_table.table_name)
        CfnOutput(self, "DDBLogTableName", value=self.ddb_log_table.table_name)
        #If no email address is configured, output the SNS topic ARN, so that they can subscribe to this SNS topic in some other manner
        if not error_email_address:
            CfnOutput(self, "SNSTopicARN", value=self.sns_topic.topic_arn)

    #Helper function to read any json file
    def read_json_file(self, file_name):
        try:
            #Add a try clause to catch any errors in opening files and raise an exception
            with open(file_name, 'r') as infile:    
                data = json.load(infile)
                return data
        except Exception as e:
            raise ValueError(f"Error in reading file {file_name}. Error: {e}")

    def validate_context_variables(self):
                
        #Stand alone checks on each context variable on its own
        if not self.boolean_validate("no_error_emails"): return False
        if not self.boolean_validate("retain_ddb_logs_table"): return False
        if not self.boolean_validate("no_buckets_configured"): return False

        if not self.email_validate("error_notification_email"): return False
        #if not self.file_validate("input_file_name"): return False

        #if not self.is_integer("max_receive_count"): return False

        #Checks on combinations of context variables:
        #input_file_name and no_buckets_configured
        cust_msg = (
                    "input_file_name context variable is required to configure buckets.\n"
                    "If you do not want to configure any buckets for prefix level key enforcement yet, please the provide context variable no_buckets_configured with a string value of 'true' or a boolean value of True.\n"
                    "Please note that if you already have some buckets configured and you deploy the stack again without the input_file_name variable, it will delete existing configurations too. So please be careful."
                    "The no_buckets_configured option is usually used only the very first time. Once you start configuring buckets, you might not want to skip passing in the input_file_name parameter\n"
        )
        if not self.either_or_validate("input_file_name", "no_buckets_configured", custom_message = cust_msg): return False

        #error_notification_email and no_error_emails
        cust_msg = (
                    "error_notification_email context variable is required to set up email notifications.\n"
                    "If you do not want to receive emails (probably because you want to handle errors differently), please pass in the no_error_emails variable with the string 'true' or a boolean True.\n"
                    "But in such a case, make sure that the SNS topic created in this stack is subscribed to for any error handling."
        )
        if not self.either_or_validate("error_notification_email", "no_error_emails", custom_message = cust_msg): return False

        #If we are here, then all checks have passed. So return True.
        return True

    def file_validate(self, context_var_name, custom_message = ""):
        context_var_value = self.node.try_get_context(context_var_name)
        print(f"validating file {context_var_value}")
        if context_var_value:
            print("Checking file")
            print(os.path.isfile(context_var_value))
            if os.path.isfile(context_var_value):
                return True
            else:
                error_msg = f'Invalid value for {context_var_name}. The value must be a valid file path. {custom_message}'
                print(error_msg)
                Annotations.of(self).add_error(error_msg)
                return False
        else:
            return True

    def email_validate(self, context_var_name, custom_message = ""):
        context_var_value = self.node.try_get_context(context_var_name)
        if context_var_value:
            if re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", context_var_value):
                return True
            else:
                error_msg = f'Invalid value for {context_var_name}. The value must be a valid email address. {custom_message}'
                Annotations.of(self).add_error(error_msg)
                return False
        else:
            return True

    # This function takes in two arguments. The first one is the name of the context variable and the second one is a boolean context variable,
    # which when set to True, the first context variable needs to be skipped. This function tests the valid combinations of these two variables:
    # 1. The boolean is set to True (or a string 'true') and the first context variable is not set.
    # 2. The boolean is not set (or set to False or a string 'false') and the first context variable is set.
    # This is used for the following two combinations:
    # - input_file_name and no_buckets_configured
    # - error_notification_email and no_error_emails
    def either_or_validate(self, context_var_name, context_var_name_bool, custom_message = ""):
        context_var_value = self.node.try_get_context(context_var_name)
        context_var_value_bool = self.node.try_get_context(context_var_name_bool)

        if context_var_value_bool:
            if (context_var_value_bool == True or
                context_var_value_bool.lower() == 'true'):
                context_bool = True
            elif (context_var_value_bool == False or
                context_var_value_bool.lower() == 'false'):
                context_bool = False
            else:
                error_msg = f"Invalid value for {context_var_name_bool}. It must be either True or False"
                Annotations.of(self).add_error(error_msg)
                return False
        else:
            context_bool = None

        if (
            (context_bool == True and not context_var_value) #Combo #1 above
            or
            (((context_bool is None) or context_bool == False) and context_var_value) #Combo #2 above
        ):
            return True
        else:
            error_msg = (
                        f"Invalid configuration. Please pass in either {context_var_name} or {context_var_name_bool}.\n"
                        f"If you pass in {context_var_name_bool} as True, then {context_var_name} should not be passed in.\n"
                        f"If you pass in {context_var_name_bool} as False or skip it, then {context_var_name} should be passed in.\n"
                        f"\n{custom_message}"
            )
            Annotations.of(self).add_error(error_msg)
            return False

    def boolean_validate(self, context_var_name, custom_message = ""):
        context_var_value = self.node.try_get_context(context_var_name)
        if context_var_value:
            if (context_var_value == True or
                context_var_value.lower() == 'true' or
                context_var_value == False or
                context_var_value.lower() == 'false'): #We need to check for both string and boolean values because while boolean values can be passed in via cdk.json, it cannot be passed in at the command line. All parameters passed in from the command line are received as strings
                return True
            else:
                error_msg = f'Invalid value for {context_var_name}. The value must be a boolean or a string "true" or "false". {custom_message}'
                Annotations.of(self).add_error(error_msg)
                return False
        else:
            return True

    def is_integer(self, context_var_name, custom_message = ""):
        context_var_value = self.node.try_get_context(context_var_name)
        if context_var_value:
            try: 
                val = int(context_var_value)
                if not (val >= 1 and val <= 1000):
                    raise ValueError(f"{context_var_name} must be an integer value between 1 and 1000")
            except ValueError:
                error_msg = f'Invalid value for {context_var_name}. It should be a a positive integer. {custom_message}'
                Annotations.of(self).add_error(error_msg)
                return False
            else:
                return True
        else:
            return True

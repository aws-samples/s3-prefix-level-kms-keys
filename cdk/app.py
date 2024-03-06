#!/usr/bin/env python3
from core_stack import CoreResourcesStack
from demo_stack import DemoResourcesStack
from aws_cdk import App, Aspects
from cdk_nag import AwsSolutionsChecks



app = App()

# Use the cdk-nag AwsSolutions Pack to validate your stack.
# Ref: https://github.com/cdklabs/cdk-nag/blob/main/RULES.md#awssolutions
Aspects.of(app).add(AwsSolutionsChecks(verbose = True))

#Core stack. This has the core set up like the Lambda function, DynamoDB tables etc.
#There will be only one instance of this stack no matter how many buckets' prefix level keys are being enforced
#But this stack will contain nested stacks (one each for each bucket whose KMS keys are being enforced.
core_stack = CoreResourcesStack(app, "S3PrefixLevelKeys")

#Demo stack - For a non-versioned bucket
demo_unversioned = DemoResourcesStack(app, f"DemoForS3PrefixLevelKeys1", bucket_versioned = False)

#If you want to test out a versioned bucket, deploy the stack DemoForS3PrefixLevelKeys2
demo_versioned   = DemoResourcesStack(app, f"DemoForS3PrefixLevelKeys2",   bucket_versioned = True)

app.synth()

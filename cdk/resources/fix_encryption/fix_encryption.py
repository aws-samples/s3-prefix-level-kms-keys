import boto3
import json
import os
import datetime

ddb = boto3.client('dynamodb')
s3 = boto3.client('s3')

def fix_encryption_if_incorrect(bucket_name, object_name, version_id = None):

    #Get the object from S3
    if version_id:
        print(f"Fixing encryption for version {version_id} of {object_name} in {bucket_name}")
        response = s3.get_object(Bucket=bucket_name, Key=object_name, VersionId=version_id)
        s3_object_path=f's3://{bucket_name}/{object_name}?versionId={version_id}'
    else:
        print(f"Fixing encryption for {object_name} in {bucket_name}")
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
        s3_object_path=f's3://{bucket_name}/{object_name}'

    #Get the encryption that is currently present on the object
    current_sse_type = response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption']
    if 'x-amz-server-side-encryption-aws-kms-key-id' in response['ResponseMetadata']['HTTPHeaders']:
        current_kms_key_arn = response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption-aws-kms-key-id'] #Though HTTPHeaders has it as key-id,  it actually is the ARN
    else:
        current_kms_key_arn = None

    #Get the information on the encryption that the object "should" have as per the DDB Mapping table
    correct_kms_key_info = get_kms_key_info_for_s3_prefix(bucket_name, object_name)

    #If this objet is not configured to have a KMS key then log the information and return.
    if not correct_kms_key_info: #No key found for the prefix
        print(f"{s3_object_path} is not configured to have a KMS key (as per the Mapping table in DynamoDB). Hence skipping")
        log_action_into_ddb(s3_object_path, current_sse_type, current_kms_key_arn, new_sse_type = 'None', new_kms_key_arn = 'None', action_taken = 'None', action_reason = 'No KMS key configured for this object\'s prefix')
        return

    #Get the details of the "correct" encryption
    new_kms_key_arn = correct_kms_key_info['kms_key_arn']
    dual_layer_encryption = correct_kms_key_info['dual_layer_encryption']
    if dual_layer_encryption:
        new_sse_type = 'aws:kms:dsse'
    else:
        new_sse_type = 'aws:kms'

    #Check if current encryption matches the encryption the object ought to have

    #If the object is already encrypted with the correct key then the information and return
    if (current_sse_type == new_sse_type and current_kms_key_arn == new_kms_key_arn):
        print(f"{s3_object_path} is already encrypted with the correct key {new_kms_key_arn}")
        log_action_into_ddb(s3_object_path, current_sse_type, current_kms_key_arn, new_sse_type, new_kms_key_arn, action_taken = 'None', action_reason = 'Already using the correct key')
    else:
        #Current encryption is incorrect.
        if (current_sse_type != new_sse_type):
            print(f"{s3_object_path} is not encrypted with the correct SSE type. x-amz-server-side-encryption is {current_sse_type} whereas it should be {new_sse_type}")
            action_reason = f'Incorrect SSE type. Was {current_sse_type} whereas it should have been {new_sse_type}'
        elif (current_kms_key_arn != new_kms_key_arn):
            print(f"{s3_object_path} is not encrypted with the correct KMS key. x-amz-server-side-encryption-aws-kms-key-id is {current_kms_key_arn} whereas it should be {new_kms_key_arn}")
            action_reason = f'Incorrect KMS key. Was {current_kms_key_arn} whereas it should hve been {new_kms_key_arn}'
        print("Hence initiating copy to encrypt with the correct key now.")

        #Initiating copy to fix the key
        if version_id:
            #Create a new version with the right encryption and log it
            #response = s3.copy(Bucket = bucket_name, Key = object_name, CopySource= {'Bucket': bucket_name, 'Key': object_name, 'VersionId': version_id}, ExtraArgs = { 'SSEKMSKeyId' : new_kms_key_arn, 'ServerSideEncryption' : new_sse_type })
            #copy_object works only on files up to 5 GB. Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/copy_object.html. But copy uses MPU under the hood if needed.
            #Using copy_object here as it returns the New Version Id.
            response=s3.copy_object(Bucket = bucket_name, Key = object_name, CopySource= {'Bucket': bucket_name, 'Key': object_name, 'VersionId': version_id}, SSEKMSKeyId = new_kms_key_arn, ServerSideEncryption = new_sse_type)
            new_version_id = response['VersionId']
            log_action_into_ddb(s3_object_path, current_sse_type, current_kms_key_arn, new_sse_type, new_kms_key_arn, action_taken = 'Initiated copy with the right key', action_reason = action_reason, new_version_id = new_version_id)

            #Delete the old version and log it
            s3.delete_object(Bucket = bucket_name, Key = object_name, VersionId = version_id)
            log_action_into_ddb(s3_object_path, current_sse_type, current_kms_key_arn, new_sse_type='N/A', new_kms_key_arn='N/A', action_taken = 'Deleted old version with incorrect encryption', action_reason = 'N/A')
        else:
            #Non-versioned. Just copy the object and log it.
            s3.copy(Bucket = bucket_name, Key = object_name, CopySource= {'Bucket': bucket_name, 'Key': object_name}, ExtraArgs = { 'SSEKMSKeyId' : new_kms_key_arn, 'ServerSideEncryption' : new_sse_type })
            log_action_into_ddb(s3_object_path, current_sse_type, current_kms_key_arn, new_sse_type, new_kms_key_arn, action_taken = 'Initiated copy with the right key', action_reason = action_reason)

    return

def log_action_into_ddb(s3_object_path, current_sse_type, current_kms_key_arn, new_sse_type, new_kms_key_arn, action_taken, action_reason, new_version_id = None):

    log_table_name = os.environ['ddb_log_table']

    if not current_kms_key_arn:
        current_kms_key_arn = 'None'

    print(f"Logging information about {s3_object_path} into {log_table_name}. {action_taken}")

    #Create the item
    item = {
            's3_object_path': {
                'S': s3_object_path
            },
            #Current timestamp with time zone
            'current_timestamp_utc': {
                'S': datetime.datetime.strftime(datetime.datetime.now(datetime.timezone.utc),"%Y-%m-%d %H:%M:%S:%f%z")
            },
            'current_sse_type': {
                'S': current_sse_type
            },
            'current_kms_key_arn': {
                'S': current_kms_key_arn
            },
            'new_sse_type': {
                'S': new_sse_type
            },
            'new_kms_key_arn': {
                'S': new_kms_key_arn
            },
            'action_taken': {
                'S': action_taken
            },
            'action_reason': {
                'S': action_reason
            }
        }

    #If this is a versioned object, then also log the new version id    
    if new_version_id:
        item['new_version_id'] = {
            'S': new_version_id
        }

    #Insert the item into the DDB table
    ddb.put_item(TableName = log_table_name, Item = item)
    return

#This function finds the best match for the prefix in the prefix-kms-key-mapping-table.
def get_kms_key_info_for_s3_prefix(bucket_name, object_name):
    print(f"Getting KMS key info for {bucket_name}/{object_name}")
    #Get all entries from the DynamoDB table whose prefixes are "<=" the object that was uploaded.
    #This ensures that most specific prefixes are listed first.
    #For example, if the table has entries for the prefixes "prefix1/b", "prefix1/bb" and "prefix1/bba",
    #then this query will fetch the prefixes in the order prefix1/bba, prefix1/bb, prefix1/b when looking for an object like prefix1/bbcdef.txt
    table_name = os.environ['ddb_mapping_table']
    result = ddb.query(TableName = table_name,
                ExpressionAttributeValues={
                    ':bucket_name': {
                        'S': bucket_name,
                    },
                    ':object_name': {
                        'S': object_name,
                    },
                },
                KeyConditionExpression='bucket_name = :bucket_name and prefix <= :object_name',
                Select='ALL_ATTRIBUTES',
                ScanIndexForward = False
            )
    print(f"Output from DDB: {result}")

    #Even when the most specific prefixes are fixed, that prefix might not match the prefix of qthe object that was uploaded.
    #Continuing the example from above, prefix1/bbcdef.txt might not have the prefix prefix1/bba (the first listed item) in it.
    #Thus, we need to find the best match for the prefix that was uploaded by going through the results one at a time.
    #This ensures that prefix1/bb is picked for this object.
    #Once a match is found, exit out of the loop.
    kms_key_info = None
    for item in result['Items']:
        if object_name.startswith(item['prefix']['S']):
            kms_key_arn = item['kms_key_arn']['S']
            dual_layer_encryption = item['dual_layer_encryption']['BOOL']
            kms_key_info = { 'kms_key_arn': kms_key_arn, 'dual_layer_encryption': dual_layer_encryption }
            break
    
    #If no KMS key is found, you could choose to raise an Exception, so that the object can be sent to the DLQ. This can be used if you need every prefix in a bucket is to be mapped to some key
    #if not kms_key_info:
    #    err_msg = f"No kms key found for object {object_name} in bucket {bucket_name}"
    #    print(err_msg)
    #    raise Exception(err_msg)

    return(kms_key_info)

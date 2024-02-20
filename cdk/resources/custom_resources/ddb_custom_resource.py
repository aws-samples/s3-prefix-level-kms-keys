import boto3
from boto3.dynamodb.conditions import Key
import datetime

#Entry point for creation, update, and deletion
#Custom resource to handle mapping table updates.
def on_event(event, context):
    print(event)
    request_type = event['RequestType']
    if request_type == 'Create': return on_create(event)
    if request_type == 'Update': return on_update(event)
    if request_type == 'Delete': return on_delete(event)
    raise Exception("Invalid request type: %s" % request_type)

#Creation
def on_create(event):
    #logical_id = event["LogicalResourceId"]
    props = event["ResourceProperties"]
    mapping_data = props["MappingData"]
    table_name = props["TableName"]
    bucket_name = props["BucketName"]
    
    print("About to create new resource with props %s" % props)

    table = get_ddb_table(table_name)
    
    #On creation just insert the data from mapping_data
    insert_ddb_items(table, bucket_name, mapping_data)
    
    print(f"Created resource with props {props}")

    return {'PhysicalResourceId': f"CRToManageMappingForBucket{bucket_name}"}

#Update
def on_update(event):
    physical_id = event["PhysicalResourceId"]
    props = event["ResourceProperties"]
    mapping_data = props["MappingData"]
    table_name = props["TableName"]
    bucket_name = props["BucketName"]

    print("About to update resource %s with props %s" % (physical_id, props))

    table = get_ddb_table(table_name)
    
    #Update DDB items that are in mapping_data...
    update_ddb_items(table, bucket_name, mapping_data)
    #... and delete items that have been removed from mapping_data
    delete_missing_ddb_items(table, bucket_name, mapping_data)

    print(f"Updated resource {physical_id} with props {props}")

    return {'PhysicalResourceId': physical_id}

#Deletion
def on_delete(event):
    physical_id = event["PhysicalResourceId"]
    props = event["ResourceProperties"]
    table_name = props["TableName"]
    bucket_name = props["BucketName"]

    print("About to delete resource %s with props %s" % (physical_id, props))

    table = get_ddb_table(table_name)
    
    #On delete, just delete the data from the DDB table for this bucket.
    delete_ddb_items(table, bucket_name)

    print(f"Deleted resource {physical_id} with props {props}")

    return {'PhysicalResourceId': physical_id}

#================
#Helper functions
#================
def insert_ddb_items(table, bucket_name, mapping_data):
    with table.batch_writer() as batch:
        for prefix in mapping_data:
            kms_key_arn = mapping_data[prefix]['kms_key_arn']
            dual_layer_encryption = mapping_data[prefix]['dual_layer_encryption']
            #Add items that will be batched
            curr_ts = datetime.datetime.strftime(datetime.datetime.now(datetime.timezone.utc),"%Y-%m-%d %H:%M:%S:%f%z")
            batch.put_item(
                            Item = {
                                    'bucket_name': bucket_name,
                                    'prefix': prefix,
                                    'kms_key_arn': kms_key_arn,
                                    'dual_layer_encryption' : dual_layer_encryption == 'true',
                                    'insert_ts': curr_ts,
                                    'last_update_ts': curr_ts
                                }
                            )
    
    print(f"Inserted {len(mapping_data)} items into DynamoDB table {table.table_name}")

def delete_ddb_items(table, bucket_name):
    items = query_table(table, key='bucket_name', value=bucket_name)
    with table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key = item)
            print(f"Deleted item {item}")
    print(f"Deleted {len(items)} items from DynamoDB table {table.table_name}")

def query_table(table, key=None, value=None):
    #Use table.query to get all items in a DynamoDB table with a given partition key
    response = table.query(
        KeyConditionExpression=Key(key).eq(value),
        Select = 'SPECIFIC_ATTRIBUTES',
        ProjectionExpression = 'bucket_name, prefix'
        )
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=Key(key).eq(value),
            ExclusiveStartKey = response['LastEvaluatedKey']
            )
        items.extend(response['Items'])
    print(f"Retrieved {len(items)} items from DynamoDB table {table.table_name}")
    return (items)

def get_ddb_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    return table

def update_ddb_items(table, bucket_name, mapping_data):
    #Go through every item in mapping data, and do an update. If the item does not exist, update_item just inserts the data
    for prefix in mapping_data:
        kms_key_arn = mapping_data[prefix]['kms_key_arn']
        dual_layer_encryption = mapping_data[prefix]['dual_layer_encryption']

        #Update the item in the DynamoDB table and ignore ConditionalCheckFailedException and ignore it.
        #This way, any entries that already match what is in the mapping data are left untouched.
        try:
            table.update_item(
                Key = {
                    'bucket_name': bucket_name,
                    'prefix': prefix
                    },
                    #Set the UPDATE expession in a way that insert_ts is updated only if the record does not already exist.
                    UpdateExpression = "SET kms_key_arn = :k, dual_layer_encryption = :d, last_update_ts = :t, insert_ts = if_not_exists(insert_ts, :t)",
                    ExpressionAttributeValues = {
                        ':k': kms_key_arn,
                        ':d': dual_layer_encryption == 'true',
                        ':t': datetime.datetime.strftime(datetime.datetime.now(datetime.timezone.utc),"%Y-%m-%d %H:%M:%S:%f%z")
                        },
                    #Add the condition that the kms_key_arn or dual_layer_encryption is different. This ensures that unchanged entries in the mapping data are left untouched in DDB.
                    ConditionExpression = "kms_key_arn <> :k or dual_layer_encryption <> :d"
            )
            print(f"Updated item {bucket_name}/{prefix} in DynamoDB table {table.table_name}")
        except table.meta.client.exceptions.ConditionalCheckFailedException: #Ignore any failures because Condition was not met. That only means that the item did not need an update.
            print(f"Item {bucket_name}/{prefix} in DynamoDB table {table.table_name} is already up to date. No update needed")
    
    print(f"Updated {len(mapping_data)} items in DynamoDB table {table.table_name}")

def delete_missing_ddb_items(table, bucket_name, mapping_data):
    #Go through mapping_data and delete items from the table that are not in mapping_data
    print(f"Deleting items from DynamoDB table {table.table_name} that are not in mapping_data for the bucket {bucket_name}")
    items = query_table(table, key='bucket_name', value=bucket_name)
    with table.batch_writer() as batch:
        for item in items:
            prefix = item['prefix']
            if prefix not in mapping_data:
                batch.delete_item(Key = item)
                print(f"Deleted item {item} from DynamoDB table {table.table_name}")

    print(f"Deleted {len(items) - len(mapping_data)} items from DynamoDB table {table.table_name}")

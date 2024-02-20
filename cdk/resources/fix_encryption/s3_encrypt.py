import urllib.parse
from fix_encryption import fix_encryption_if_incorrect
import json

#Handles messages from the SQS queue.
def lambda_handler(event, context):
    print(f"Received event: {event}")
    batch_item_failures = []

    # Get the messages from the event and process them. If successful, append to processed_messages, if not append to failed_messages else append to failed_messages
    for sqs_msg in event['Records']: #When updating the event notification configuration on a bucket, a test event is sent. In that case, it will not have a "Records" item in the "body". This if statement handles that
        s3_recs = json.loads(sqs_msg['body'])
        if 'Records' in s3_recs:
            #Each rec represents a single object that has been uploaded to the bucket
            for rec in s3_recs["Records"]:         #As of today only a separate event is triggered for each objet in S3, but creating a loop just to be sure.
                bucket_name = rec['s3']['bucket']['name']
                object_name = urllib.parse.unquote_plus(rec['s3']['object']['key'], encoding='utf-8')
                version_id = None
                if 'versionId' in rec['s3']['object']:
                    version_id = rec['s3']['object']['versionId']
                try:
                    #This is the call that fixes the encryption of the object. If the encryption is incorrect, it will fix it. If it is correct, it will do nothing.
                    fix_encryption_if_incorrect(bucket_name, object_name, version_id=version_id)
                    print(f"Processed message: {rec}")
                except Exception as e:
                    batch_item_failures.append({'itemIdentifier': sqs_msg['messageId']})
                    print(f"Failed to process message: {rec}")
                    print(f"Error: {e}")
        else:
            print(f"No records found in {sqs_msg}. Hence ignored")
            continue

    #If there have been failures, report back with a status of 500. If not report back with 200
    if len(batch_item_failures) == 0:
        print(f"All objects successfully processed")
        return {
            "statusCode": 200,
        }
    else:
        response = {
            "statusCode": 500,
            "batchItemFailures": batch_item_failures
        }
        print(f"There were some failures. Returning {response}")
        return(response)

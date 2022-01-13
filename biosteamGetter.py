# import json

# def lambda_handler(event, context):
#     # TODO implement
#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }

import json
import boto3

dynamodb = boto3.client('dynamodb')

# =============================================================================
#biosteamGetter function gets data stored in dynamoDB table "biosteamResults" by 
#jobId lookup
# =============================================================================

def lambda_handler(event, context):
    #Read postId from event 
    print(event)
    jobId = event['jobId']  
    print('jobId', jobId)
    #check if item exists and get corresponting item or throw error
    try:
        response = dynamodb.get_item(
            TableName='biosteam-results',
            Key={
              'jobId': {'S': jobId}
            }
        )

        item = response['Item']
    except: 
        item = "no data"
    print('item ', item)
    return {
        'statusCode': 200,
        "headers": {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps({
            'item': item,
            'jobId': jobId,
        })
    }

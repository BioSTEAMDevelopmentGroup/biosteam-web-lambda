import json
import uuid
import boto3
import time 

# Define the client to interact with AWS Lambda
client = boto3.client('lambda')

# =============================================================================
# Lambda Function: accepts biosteam data from api gateway, routes values to 
# corresponding biosteam simulation functions and returns jobId and jobTimestamp
# =============================================================================

def lambda_handler(event, context):
    # read input biosteam data
    params = event['params']
    samples = event['samples']
    
    # Create UUID (use type 4 for generic uuid)
    jobId = str(uuid.uuid4())
    
    # Create timestamp for transaction, in unix format
    jobTimestamp = time.time()

    # =============================================================================
    # Function router: send input biosteam data to corresponding function 
    # ============================================================================= 
    
    # Cornstover uncertainty function
    response = client.invoke(
        FunctionName = 'arn:aws:lambda:us-east-1:499290667614:function:uncertainty',
        InvocationType = 'Event',
        Payload = json.dumps({
            'jobId': jobId,
            'jobTimestamp': jobTimestamp,
            'params': params,
            'samples': samples,
        })
    )
    
    return {
        'statusCode': 200,
        "headers": {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps({
            'jobId': jobId,
            'jobTimestamp': str(int(jobTimestamp)),
            'params': params,
            'status': 'job being proccessed'
        })
    }
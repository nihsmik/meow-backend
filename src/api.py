import base64
import boto3
import json
from operator import itemgetter

Dynamo = boto3.client('dynamodb')
TABLE_NAME = "meow-production-media"
DEFAULT_COUNT = 6
DEFAULT_AFTER = 0

def lambda_handler(event, context):
    count = DEFAULT_COUNT
    after = DEFAULT_AFTER
    event_count = event.get('queryStringParameters', {}).get('count')
    event_after = event.get('queryStringParameters', {}).get('after')
    if event_count:
        count = int(event_count)
    if event_after:
        after = int(event_after)

    data = sorted(
        [{
            'image_url': i['image_url']['S'],
            'profile_url': i['profile_url']['S'],
            'username': i['username']['S'],
            'expired_at': i['expired_at']['N']
        } for i in get_all_items()],
        key=itemgetter('expired_at', 'image_url'),
        reverse=True
    )

    return respond(None, {
        'data': data[after:after+count],
        'after': after+count
    })

def get_all_items(items=[], last_evaluated_key=None):
    if last_evaluated_key:
        result = Dynamo.scan(
            TableName=TABLE_NAME,
            ExclusiveStartKey=last_evaluated_key
        )
    else:
        result = Dynamo.scan(TableName=TABLE_NAME)

    after = result.get('LastEvaluatedKey')
    if after:
        return get_all_items(items + result['Items'], after)
    return items + result['Items']


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
    }

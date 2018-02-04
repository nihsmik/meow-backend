from botocore.vendored import requests
import re
import boto3
from datetime import datetime

Dynamo = boto3.client('dynamodb')
PHOTOS_REGEX = r'"src":"(https:\/\/[^,]+640x640[^,]+.jpg)'
PROFILE_REGEX = r'<meta property="og:image" content="(.+)"'
TABLE_NAME = 'meow-production-media'
IMAGE_LIFE_SECONDS = 259200 # 3 days


def lambda_handler(event, context):
    username = event["username"]
    response = requests.get(
        "https://www.instagram.com/{}/".format(event["username"]
    )).text
    expired_at = str(
        int(datetime.utcnow().strftime('%s')) + IMAGE_LIFE_SECONDS
    )
    items = [{
        'PutRequest': {
            'Item': {
                'image_url': {'S': image_url},
                'type': {'S': 'image'},
                'id': {'S': get_id(image_url)},
                'username': {'S': username},
                'profile_url': {
                    'S': re.search(PROFILE_REGEX, response).group(1)
                },
                'expired_at': {'N': expired_at}
            }
        }
    } for image_url in get_new_image_urls(response)]

    if items:
        Dynamo.batch_write_item(RequestItems={TABLE_NAME: items})

    return items


def get_id(image_url):
    return re.search(r'\/(\w+).jpg', image_url).group(1)


def get_new_image_urls(response):
    image_urls = [m.group(1) for m in re.finditer(PHOTOS_REGEX, response)]
    existings = Dynamo.batch_get_item(RequestItems={
        TABLE_NAME: {
            'Keys': [{
                'id': {'S': get_id(u)}
            } for u in image_urls]
        }
    })['Responses'][TABLE_NAME]
    return [
        url for url in image_urls
        if re.search(r'\/(\w+).jpg', url).group(1) not in [
            item['id']['S'] for item in existings
        ]
    ]

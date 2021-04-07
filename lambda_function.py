import boto3
import json
import logging
import urllib3
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')

def send_request(bucket, photo, head_object, labels):
    host = 'https://search-photos1-qt6drdhhpna5jdwct6mp5u6jfu.us-east-1.es.amazonaws.com'
    index = 'photos1'
    url = host + '/' + index + '/_doc'
    headers = urllib3.make_headers(basic_auth='user:Password123.')
    headers["Content-Type"] = "application/json"
    http = urllib3.PoolManager()
    body = {
        "objectKey": photo,
        "bucket": bucket,
        "createdTimestamp": head_object["LastModified"].strftime("%Y-%m-%dT%H:%M:%S"),
        "labels": labels
    }

    logger.debug(f"SENDING REQUEST")
    r = http.request('POST', url, headers=headers, body=json.dumps(body))
    logger.debug(f"RESPONSE: {r.data}")
    resp = json.loads(r.data.decode('utf-8'))
    logger.debug(f"RESPONSE2: {resp}")

def add_rekognition_labels(bucket, photo, labels):
    response = rekognition.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': photo}}, MaxLabels=10)
    my_labels = response['Labels']
    for label in my_labels:
        labels.append(label["Name"])

def get_custom_labels(metadata, labels):
    customLabels = metadata["customlabels"].split(',')
    print(customLabels)
    for customLabel in customLabels:
        labels.append(customLabel.strip())

def process_image(bucket, photo):
    logger.debug(f"BUCKET: {bucket}")
    logger.debug(f"PHOTO: {photo}")

    head_object = s3.head_object(Bucket=bucket, Key=photo)
    logger.debug(f"HEAD_OBJ: {head_object}")

    metadata = head_object["Metadata"]
    labels = []

    if "customlabels" in metadata:
        get_custom_labels(metadata, labels)
    logger.debug(f"LABELS1: {labels}")

    add_rekognition_labels(bucket, photo, labels)
    logger.debug(f"LABELS2: {labels}")
    
    send_request(bucket, photo, head_object, labels)

    return labels

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    photo = unquote_plus(event["Records"][0]["s3"]["object"]["key"])
    labels = process_image(bucket, photo)

    return {
        'statusCode': 200,
        'body': labels
    }
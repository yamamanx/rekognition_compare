import logging.config
import traceback
import boto3
import urllib.parse
import json
import os
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

target_list = [
    'kureopatra',
    'syotokutaishi',
    'himiko',
    'mozart',
    'bezos',
    'vogerusu',
    'jobs'
]


def lambda_handler(event, context):
    try:
        client = boto3.client('rekognition')

        bucket = boto3.resource('s3').Bucket(
            os.environ['BUCKET_NAME']
        )

        logger.info(event)

        for record in event['Records']:

            bucket_name = record['s3']['bucket']['name']
            key = urllib.parse.unquote(record['s3']['object']['key'])
            logger.info(key)

            for target in target_list:
                target_key = '{prefix}/{file}.jpg'.format(
                    prefix='target',
                    file=target
                )
                logger.info(target_key)

                try:
                    response = client.compare_faces(
                        SourceImage={
                            'S3Object': {
                                'Bucket': bucket_name,
                                'Name': key
                            }
                        },
                        TargetImage={
                            'S3Object': {
                                'Bucket': bucket_name,
                                'Name': target_key
                            }
                        },
                        SimilarityThreshold=0
                    )

                    logger.info(response)

                    similarity = response['FaceMatches'][0]['Similarity']

                    reponse_json = {
                        'source': key,
                        'target': target,
                        'similarity': similarity
                    }

                    bucket.put_object(
                        Key='{prefix}/{key}_{target}.json'.format(
                            prefix=datetime.now().strftime('%Y/%m/%d/%H/%M/%S'),
                            key=key,
                            target=target
                        ),
                        Body=json.dumps(reponse_json),
                        ContentType='text/json'
                    )

                except:
                    logger.error(traceback.format_exc())

    except:
        logger.error(traceback.format_exc())

    finally:
        return event

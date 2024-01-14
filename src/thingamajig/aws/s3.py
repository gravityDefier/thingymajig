#!env python3 -u

import sys
import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import ClientError
import logging
import time

logging.basicConfig(encoding='utf-8', level=logging.DEBUG)
root_logger = logging.getLogger()
root_logger.propagate = True
root_logger.handlers.clear()

logger = logging.getLogger(__name__)

logger.propagate = True
logger.handlers.clear()

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)

# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)


def get_client(config=None):
    logger.debug('generating client')
    if config is None:
        config = Config(
            retries={
                'max_attempts': 10,
                'mode': 'standard'
            }
        )
        return boto3.client('s3', config=config)
    else:
        return boto3.client("s3", config=config)


def list(client, bucket, prefix='', time_limit=0, object_limit=0, attempts_limit=10):
    logger.debug(f'listing {bucket} prefix {prefix}')
    start_time = time.time()

    attempts = 0 

    while attempts < attempts_limit:
        attempts +=1
        try:
            result = client.list_objects_v2(
                Bucket=bucket,
                EncodingType='url',
                Prefix=prefix
            )

            object_list = []

            more_to_get = True

            continuation_token = ''
            returned_count = 0

            while more_to_get:

                if result is None:
                    raise Exception("ERROR - No result encountered")

                if 'Contents' in result:
                    for obj in result['Contents']:
                        returned_count += 1
                        object_list.append(obj)
                else:
                    more_to_get = False
                    break

                if 'NextContinuationToken' in result:
                    logger.debug(f'cont token detected')
                    continuation_token = result['NextContinuationToken']
                    result = client.list_objects_v2(Bucket=bucket,
                                                    EncodingType='url',
                                                    Prefix=prefix, ContinuationToken=continuation_token)
                else:
                    logger.debug(f'we are done')
                    more_to_get = False

                if (time_limit > 0 and time.time() - start_time > time_limit):
                    logger.info(f'Finishing prematurely due to provided max_time argument')
                    break
                if (object_limit > 0 and len(object_list) > object_limit):
                    logger.info(f'Finishing prematurely due to object limit being met or exceeded')
                    break
            if more_to_get is False:
                break 
        except botocore.exceptions.ClientError as exc:
            logger.warn(f'Encountered {exc!r} exception')
            time.sleep(2 * attempts)

    logger.debug(f'count returned: {returned_count}')
    return object_list

def save_file(client, bucket, key, data):
    client.put_object(Body=data, Bucket=bucket, Key=key)

def upload(client, bucket, key, file):
    try:
        response = s3_client.upload_file(file, bucket, key)
    except ClientError as e:
        logging.error(e)
        return False
    return True

class s3:
    def __init__(self) -> None:
        self.client = get_client()

        pass

    def list_prefix(self, bucket, prefix='', time_limit=0, object_limit=0, attempts_limit=10):

        return list(self.client, bucket, prefix='', time_limit=0, object_limit=0, attempts_limit=10)
    
    def save_file(self, client, bucket, key, data):
        return save_file(client, bucket, key, data)
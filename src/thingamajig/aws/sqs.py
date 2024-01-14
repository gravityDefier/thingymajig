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
        return boto3.client('sqs', config=config)
    else:
        return boto3.client("sqs", config=config)
    
def send(client, queue_url, msg_attributes=None, msg_body=''):

    if msg_attributes is None:
        msg_attributes = {}

    response = client.send_message(
    QueueUrl=queue_url,
    DelaySeconds=10,
    MessageAttributes=msg_attributes,
    MessageBody=msg_body

    if 'MessageId' in response:
        logger.debug('sqs message id {messageId} sent')

)
    
def get(client,queue_url, number_messages, msg_attributes ):
    # Receive message from SQS queue
    response = client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0

    )

    received = []

    for msg in response['Messages']:
        received.append({ 'body' : msg,
                          'handle' : msg['ReceiptHandle']
                          })


    return received

def delete(client, queue_url, receipt_handle):

    to_delete = []

    if isinstance(receipt_handle, list):
        for handle in receipt_handle:
            to_delete.append(handle)
    if isinstance(receipt_handle, str):
        to_delete.append(handle)

    for msg in to_delete:
        # Delete received message from queue
        client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=msg
        )
    print('Received and deleted message: %s' % message)

class sqs:
    def __init__(self) -> None:
        self.client = get_client()

        pass

    def get(self, client,queue_url, number_messages, msg_attributes):
        return get(client,queue_url, number_messages, msg_attributes)
    
    def send(self, client, queue_url, msg_attributes=None, msg_body=''):
        return send(client, queue_url, msg_attributes=None, msg_body='')
    
    def delete(self, client, queue_url, receipt_handle):
        return delete(client, queue_url, receipt_handle)
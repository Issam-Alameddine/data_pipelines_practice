import json
import logging
import boto3
logger = logging.getLogger(__name__)

kinesis = boto3.client('kinesis')

stream_batch = []

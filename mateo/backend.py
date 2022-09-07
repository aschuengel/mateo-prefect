import json
import time
from typing import Optional
import boto3

from mateo.model import MateoInboundMessage, MateoState, MateoStateEnum

IN_QUEUE_URL = 'https://sqs.eu-west-1.amazonaws.com/789850903462/prefect-in'
OUT_QUEUE_URL = 'https://sqs.eu-west-1.amazonaws.com/789850903462/prefect-out'
TABLE_NAME = 'mateo'


def send_inbound_message(message: MateoInboundMessage):
    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl=IN_QUEUE_URL,
        MessageBody=message.json()
    )


def read_inbound_messages() -> list[MateoInboundMessage]:
    sqs = boto3.client('sqs')
    result = sqs.receive_message(
        QueueUrl=IN_QUEUE_URL
    )
    if 'Messages' not in result:
        return []
    messages = []
    for message in result['Messages']:
        body = message['Body']
        body = json.loads(body)
        messages.append(MateoInboundMessage(
            table_name=body['table_name'], correlation_id=body['correlation_id'], receipt_handle=message['ReceiptHandle']))
    return messages


def save_state(state: MateoState, expiration_timestamp: float = None):
    if expiration_timestamp is None:
        expiration_timestamp = time.time() + 5 * 24 * 3600
    dynamodb = boto3.client('dynamodb')
    item = {
        'correlation_id': {'S': state.correlation_id},
        'table_name': {'S': state.table_name},
        'state': {'S': state.state.value},
        'expiration_timestamp': {'N': str(expiration_timestamp)}
    }
    if state.end_of_processing_timestamp is not None:
        item['end_of_processing_timestamp'] = {
            'N': str(state.end_of_processing_timestamp)}
    if state.start_of_processing_timestamp is not None:
        item['start_of_processing_timestamp'] = {
            'N': str(state.start_of_processing_timestamp)}

    dynamodb.put_item(
        TableName=TABLE_NAME,
        Item=item
    )


def read_state(correlation_id: str) -> Optional[MateoState]:
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.get_item(
        TableName=TABLE_NAME,
        Key={
            'correlation_id': {'S': correlation_id}
        }
    )
    if not 'Item' in response:
        return None
    item = response['Item']
    return MateoState(table_name=item['table_name']['S'], correlation_id=correlation_id, state=MateoStateEnum(item['state']['S']))


def delete_inbound_message(receipt_handle: str):
    sqs = boto3.client('sqs')
    sqs.delete_message(
        QueueUrl=IN_QUEUE_URL,
        ReceiptHandle=receipt_handle
    )


def send_outbound_message(body: str):
    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl=OUT_QUEUE_URL,
        MessageBody=json.dumps(body)
    )

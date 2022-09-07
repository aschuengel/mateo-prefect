import boto3


TABLE_NAME = 'mateo'


def create_sqs_queues():
    sqs = boto3.client('sqs')
    sqs.create_queue(
        QueueName='prefect-in'
    )
    sqs.create_queue(
        QueueName='prefect-out'
    )


def create_dynamodb_table():
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.list_tables()
    table_names = response['TableNames']
    if TABLE_NAME in table_names:
        print(f'Table {TABLE_NAME} already exists')
        return

    dynamodb.create_table(
        TableName=TABLE_NAME,
        AttributeDefinitions=[
            {
                'AttributeName': 'correlation_id',
                'AttributeType': 'S'
            }
        ],
        KeySchema=[
            {
                'AttributeName': 'correlation_id',
                'KeyType': 'HASH'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    waiter = dynamodb.get_waiter('table_exists')
    waiter.wait(
        TableName=TABLE_NAME
    )
    print(f'Table {TABLE_NAME} has been created')

def update_dynamodb_ttl():
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.describe_time_to_live(
        TableName=TABLE_NAME
    )
    if response['TimeToLiveDescription']['TimeToLiveStatus'] == 'DISABLED':
        dynamodb.update_time_to_live(
            TableName=TABLE_NAME,
            TimeToLiveSpecification={
                'Enabled': True,
                'AttributeName': 'expiration_timestamp'
            }
        )
        print(f'Updated TTL on table {TABLE_NAME}')
    else:
        print(f'TTL already active on table {TABLE_NAME}')

if __name__ == '__main__':
    create_sqs_queues()
    create_dynamodb_table()
    update_dynamodb_ttl()

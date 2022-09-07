import asyncio
import time
from prefect import flow, task, get_run_logger

from mateo.backend import delete_inbound_message, read_inbound_messages, save_state
from mateo.model import MateoInboundMessage, MateoState, MateoStateEnum


@task
def process_message(message: MateoInboundMessage):
    logger = get_run_logger()
    table_name = message.table_name
    correlation_id = message.correlation_id
    logger.info(
        f'Set state of {correlation_id} to processing, table: {table_name}')
    state = MateoState(correlation_id=correlation_id,
                       table_name=table_name,
                       state=MateoStateEnum.PROCESSING,
                       start_of_processing_timestamp=time.time())
    save_state(state)
    time.sleep(10)
    logger.info(f'Update state of {correlation_id} to completed')
    state.state = MateoStateEnum.COMPLETED
    state.end_of_processing_timestamp = time.time()
    save_state(state)
    logger.info(f'Delete message {correlation_id} from SQS inbound queue')
    delete_inbound_message(message.receipt_handle)
    #logger.info(f'Send message {body} to SQS outbound queue')
    # send_outbound_message(body)


@flow
async def main():
    for _ in range(10):
        messages = read_inbound_messages()
        for message in messages:
            process_message.submit(message)
        await asyncio.sleep(5)


if __name__ == '__main__':
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass

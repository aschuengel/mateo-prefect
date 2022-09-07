import asyncio
import time
import uuid
from prefect import flow, get_run_logger, task

from mateo.backend import read_state, send_inbound_message
from mateo.model import MateoInboundMessage, MateoStateEnum


@task
async def wait_for_mateo(id: str, max_wait: float = 100) -> None:
    logger = get_run_logger()
    start = time.time()
    logger.info(f'Wait for {id}. Max. wait time is {max_wait} sec.')
    while True:
        state = read_state(id)
        if state is None:
            logger.info(f'No state yet for {id}')
        elif state.state != MateoStateEnum.COMPLETED:
            logger.info(f'State of {id}: {state.state}')
        else:
            logger.info(f'Finished {id}')
            return
        if time.time() - start > max_wait:
            raise Exception(f'Max. wait time of {max_wait} sec. exceeded')
        await asyncio.sleep(5)


@task
def send_mateo_message(table_name: str) -> str:
    logger = get_run_logger()
    message = MateoInboundMessage(
        table_name=table_name, correlation_id=str(uuid.uuid4()))
    logger.info(f'Sending payload {message} to SQS inbound queue')
    send_inbound_message(message)
    return message.correlation_id


@flow
async def test() -> None:
    table_names = ['MARA', 'KNA1', 'EQUI', 'BKPF', 'LFA1']
    logger = get_run_logger()
    logger.info(f'Tables: {table_names}')
    ids = [send_mateo_message(table_name) for table_name in table_names]
    tasks = [wait_for_mateo(id) for id in ids]
    logger.info(f'Tasks: {tasks}')
    await asyncio.gather(*tasks)
    logger.info('All tasks completed')


if __name__ == '__main__':
    asyncio.run(test())

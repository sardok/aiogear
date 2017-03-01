import sys
import os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from functools import partial
import asyncio
import argparse
from aiogear import Worker, Client


def parse_args():
    args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--addr', default='127.0.0.1', help='Gearman host address.')
    parser.add_argument('-p', '--port', default=4730, type=int, help='Gearman port number.')
    return parser.parse_args(args)


async def sleep(job_info):
    sleep_time = int(job_info.workload)
    print(job_info)
    print('Sleeping for ', sleep_time, ' seconds')
    await asyncio.sleep(sleep_time)
    print('Sleep is done')


async def register_worker(loop, addr, port):
    factory = lambda: Worker(sleep, loop=loop)
    _, worker = await loop.create_connection(factory, addr, port)
    return worker

async def connect(loop, addr, port):
    client = Client()
    await loop.create_connection(lambda: client, addr, port)
    return client


def job_is_complete(job_created, f):
    print('Created Job ', job_created, ' is finished')


async def main(loop, addr, port):

    # Connects worker & register function
    worker = await register_worker(loop, addr, port)

    # Connect as client
    client = await connect(loop, addr, port)

    # Run sleep task and wait it
    job_created = await client.submit_job('sleep', '5')
    f = client.wait_job(job_created.handle)
    f.add_done_callback(partial(job_is_complete, job_created))
    await f

    await worker.shutdown()
    client.disconnect()

    loop.stop()

if __name__ == '__main__':
    args = parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, args.addr, args.port))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.close()

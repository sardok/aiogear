import sys
import os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
import argparse
from aiogear import Worker


def parse_args():
    args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--addr', default='127.0.0.1', help='Gearman host address.')
    parser.add_argument('-p', '--port', default=4730, type=int, help='Gearman port number.')
    return parser.parse_args(args)


def reverse(job_info):
    print(job_info)
    return job_info.workload[::-1]


def main(addr, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_connection(lambda: Worker(reverse, loop=loop), addr, port)
    _, worker = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(worker.shutdown())
    loop.close()

if __name__ == '__main__':
    args = parse_args()
    main(args.addr, args.port)

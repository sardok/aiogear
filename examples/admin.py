import sys
import os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pprint
import asyncio
import argparse
from aiogear import Admin


def parse_args():
    args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--addr', default='127.0.0.1', help='Gearman host address.')
    parser.add_argument('-p', '--port', default=4730, type=int, help='Gearman port number.')
    return parser.parse_args(args)


async def connect(loop, addr, port):
    admin = Admin(loop=loop)
    await loop.create_connection(lambda: admin, addr, port)
    return admin

async def main(loop, addr, port):

    # Connect as admin
    admin = await connect(loop, addr, port)

    # Status
    status = await admin.status()
    pprint.pprint(status)

    # Workers
    workers = await admin.workers()
    pprint.pprint(workers)

    # Version
    result = await admin.version()
    pprint.pprint(result)

    # Verbose
    result = await admin.verbose()
    pprint.pprint(result)

    loop.stop()

if __name__ == '__main__':
    args = parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, args.addr, args.port))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.close()

import re
import asyncio
import logging
from collections import deque
from io import StringIO

logger = logging.getLogger(__name__)


class Admin(asyncio.Protocol):
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.transport = None
        self.waiters = deque()
        self.buffer = b''
        self.linesep = b'\n'
        self.structure_sep = self.linesep + b'.' + self.linesep
        self.telnet_sep = b'\r\n'

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport = None

    def data_received(self, data):
        self.buffer += data
        while self.buffer:
            try:
                pair = self.waiters[0]
                f, eol = pair
            except IndexError:
                logger.warning('Unexpected msg %s', self.buffer)
                break

            buffer = self.buffer
            index = buffer.find(eol)
            if index < 0:
                break

            msg = buffer[:index]
            self.buffer = buffer[index+len(eol):]
            self.waiters.popleft()
            f.set_result(msg.decode('utf-8'))

    async def workers(self):
        resp = await self.send_and_wait_resp(b'workers', self.structure_sep)
        sio = StringIO(resp)
        workers = []
        for line in sio:
            client, funcs = line.split(':')
            try:
                fd, ip, name = re.findall(r'(\S+)', client)
                funcs = re.findall(r'(\S+)', funcs)
            except ValueError:
                logger.warning('Malformed worker line %s', line)
                continue

            worker = dict(fd=int(fd), ip_address=ip, name=name, functions=funcs)
            workers.append(worker)
        return workers

    async def status(self):
        resp = await self.send_and_wait_resp(b'status', self.structure_sep)
        sio = StringIO(resp)
        status = []
        for line in sio:
            func, *stats = re.findall(r'(\S+)', line)
            total, running, available = map(int, stats)
            entry = dict(function=func, total=total, running=running, available_workers=available)
            status.append(entry)
        return status

    async def maxqueue(self, function, size=None):
        try:
            function = function.encode('ascii')
        except AttributeError:
            pass
        data = [b'maxqueue', function]
        if size is not None:
            try:
                size = str(int(size)).encode('ascii')
                data.append(size)
            except (TypeError, ValueError):
                pass
            try:
                if len(size) != 3:
                    raise RuntimeError
                size = [str(int(s)).encode('ascii') for s in size]
                data.extend(size)
            except:
                raise RuntimeError(
                    'Unsupported size parameter {}, it must be either int or list of 3 values')
        return await self.send_and_wait_resp(b' '.join(data), self.telnet_sep)

    async def shutdown(self, graceful=False):
        data = [b'shutdown']
        if graceful:
            data.append(b'graceful')
        return await self.send_and_wait_resp(b' '.join(data), self.telnet_sep)

    async def version(self):
        return await self.send_and_wait_resp(b'version', self.linesep)

    async def verbose(self):
        return await self.send_and_wait_resp(b'verbose', self.linesep)

    def send_and_wait_resp(self, cmd, expected_eol):
        self.transport.write(cmd + self.linesep)
        f = self.loop.create_future()
        pair = (f, expected_eol)
        self.waiters.append(pair)
        return f

    def disconnect(self):
        try:
            self.transport.close()
        except AttributeError:
            pass

    close = disconnect

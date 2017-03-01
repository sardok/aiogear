import struct
import asyncio
import inspect
from aiogear import PacketType


class GearmanServerMock(asyncio.Protocol):
    def __init__(self, loop, messaging=None):
        self.loop = loop
        self.messaging = messaging or {}
        self.transport = None
        self.data = b''

    def extract_packets(self):
        packets = []
        while True:
            data = self.data
            header = '>4sII'
            offset = struct.calcsize(header)
            try:
                _, num, size = struct.unpack(header, data[:offset])
            except struct.error:
                break

            packets.append(PacketType(num))
            if len(data[offset:]) < size:
                self.data = data[offset:]
                break
            else:
                self.data = data[offset + size:]
        return packets

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.data += data
        for incoming in self.extract_packets():
            try:
                response_or_cb = self.messaging[incoming]
            except KeyError:
                continue

            if inspect.isfunction(response_or_cb):
                response_or_cb(incoming)
            else:
                serialized = self.serialize_response(*response_or_cb)
                self.transport.write(serialized)

    @staticmethod
    def serialize_response(packet, *args):
        magic = b'\0RES'
        delimiter = b'\0'
        payload = delimiter.join([a.encode('ascii') for a in args])
        return struct.pack('>4sII', magic, packet.value, len(payload)) + payload


def run_mock_server(loop, run_port, **kw):
    return loop.create_server(
        lambda: GearmanServerMock(loop, **kw), '127.0.0.1', run_port)


async def connect_worker(loop, server_port, worker_factory):
    _, worker = await loop.create_connection(worker_factory, '127.0.0.1', server_port)
    return worker

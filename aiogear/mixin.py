import struct
import asyncio
import logging
from functools import partial
from aiogear.packet import Type
from aiogear.utils import to_bool
from aiogear.response import Noop, NoJob, JobCreated
from aiogear.response import WorkComplete, WorkFail, WorkException
from aiogear.response import JobAssign, JobAssignUniq, JobAssignAll

logger = logging.getLogger(__file__)


class GearmanProtocolMixin:
    _REQ_MAGIC = b'\0REQ'
    _RES_MAGIC = b'\0RES'
    _delimiter = b'\0'
    _data = b''

    def __init__(self, loop=None):
        super(GearmanProtocolMixin, self).__init__()
        self.loop = loop or asyncio.get_event_loop()
        self._serializers = {
            Type.CAN_DO_TIMEOUT: lambda *xs: self._join(*[str(x) for x in xs])
        }
        self._deserializers = {
            Type.JOB_ASSIGN: lambda x: JobAssign(
                *[a.decode('utf8') for a in self._split(x, maxsplit=2)]),
            Type.JOB_ASSIGN_UNIQ: lambda x: JobAssignUniq(
                *[a.decode('utf8') for a in self._split(x, maxsplit=3)]),
            Type.JOB_ASSIGN_ALL: lambda x: JobAssignAll(
                *[a.decode('utf8') for a in self._split(x, maxsplit=4)]),
            Type.STATUS_RES: self._status_res_handler,
            Type.STATUS_RES_UNIQUE: self._status_res_handler,
            Type.WORK_COMPLETE: lambda x: WorkComplete(*[a.decode('utf8') for a in self._split(x)]),
            Type.WORK_FAIL: lambda x: WorkFail(self._split(x)[0].decode('utf8')),
            Type.WORK_EXCEPTION: lambda x: WorkException(
                *[a.decode('utf8') for a in self._split(x)]),
            Type.ERROR: self._error_handler,
            Type.NO_JOB: lambda _: NoJob(),
            Type.NOOP: lambda _: Noop(),
            Type.JOB_CREATED: lambda x: JobCreated(x.decode('utf8')),
        }
        self._request = partial(self._pack, self._REQ_MAGIC)
        self._response = partial(self._pack, self._RES_MAGIC)
        self._registers = []

    def serializer(self, packet):
        return self._serializers.get(packet, self._join)

    def _unpack(self, data):
        fmt = '>4sII'
        fmt_sz = struct.calcsize(fmt)
        magic, packet_num, sz = struct.unpack(fmt, data[:fmt_sz])
        begin, end = fmt_sz, fmt_sz + sz
        return Type(packet_num), data[begin:end], end

    def _pack(self, magic, packet, payload=b''):
        assert isinstance(packet, Type)
        length = len(payload)
        packed = struct.pack('>4sII', magic, packet.value, length)
        if isinstance(payload, str):
            payload = payload.encode('ascii')
        return packed + payload

    def serialize_response(self, packet_type, *args):
        handler = self.serializer(packet_type)
        payload = handler(*args)
        return self._response(packet_type, payload)

    def serialize_request(self, packet_type, *args):
        handler = self.serializer(packet_type)
        payload = handler(*args)
        return self._request(packet_type, payload)

    def _split(self, data, delimiter=None, maxsplit=-1):
        delimiter = delimiter or self._delimiter
        return data.split(delimiter, maxsplit)

    def _join(self, *args, delimiter=None):
        delimiter = delimiter or self._delimiter
        args = [a.encode('ascii') if isinstance(a, str) else a for a in args]
        return delimiter.join(args)

    def do_register(self, callback, *packets):
        key = packets
        entry = (key, callback)
        self._registers.append(entry)

    def wait_for(self, *packets, return_response=False):
        f = self.loop.create_future()

        def cb(*data):
            packet_type, response = data
            if return_response:
                f.set_result((packet_type, response))
            else:
                f.set_result(response)
        self.do_register(cb, *packets)
        return f

    def get_registered(self, packet):
        index = None
        for i, (key, _) in enumerate(self._registers):
            if packet in key:
                index = i
                break
        if index is not None:
            _, cb = self._registers.pop(index)
            return cb

    def _cast_args(self, args, casters):
        return [f(x) for f, x in zip(casters, args)]

    def _status_res_handler(self, data):
        args = self._split(data)
        casters = [lambda x: x.decode('utf8'), to_bool, to_bool, int, int, int]
        return self._cast_args(args, casters)

    def _error_handler(self, data):
        args = self._split(data)
        casters = [int, lambda x: x.decode('utf8')]
        return self._cast_args(args, casters)

    def data_received(self, data):
        self._data += data
        while self._data:
            try:
                packet, payload, offset = self._unpack(self._data)
            except struct.error:
                # not enough data in the buffer
                break
            handler = self._deserializers.get(packet, lambda x: x)
            args = handler(payload)
            cb = self.get_registered(packet)
            if cb:
                cb(packet, args)
            else:
                logger.warning('Received un-expected message from server: %s (%r)', packet, args)
            self._data = self._data[offset:]

    def _send(self, data):
        if self.transport:
            self.transport.write(data)

    def send(self, packet, *args):
        data = self.serialize_request(packet, *args)
        self._send(data)

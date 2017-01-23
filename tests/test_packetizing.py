import pytest
import asyncio
from struct import pack
from functools import partial
from aiogear.mixin import GearmanProtocolMixin
from aiogear.packet import Type
from aiogear.response import NoJob, JobCreated, JobAssign, Noop, WorkComplete


_req = b'\0REQ'
_res = b'\0RES'


def _pack(ptype, fmt, *args):
    return pack('>4s' + fmt, ptype, *args)

_pack_req = partial(_pack, _req)
_pack_res = partial(_pack, _res)


class TestPacketizing:
    """
    Test messages documented in Binary Protocol Example section of
    http://gearman.org/protocol/
    """
    result = ''
    protocol = None

    def given_protocol_params(self, event_loop=None):
        self.protocol = GearmanProtocolMixin(loop=event_loop)

    @pytest.mark.asyncio
    async def when_data_received(self, data, event_loop, register_for=None, timeout=0.1):
        # Register for all packet type, so not given explicitly
        register_for = register_for or [x for x in Type]

        f = self.protocol.wait_for(*register_for)
        self.protocol.data_received(data)
        self.result = await asyncio.wait_for(f, timeout, loop=event_loop)

    def then_expect_result(self, expected):
        assert expected == self.result

    @pytest.mark.parametrize('packet_type,args,expected', [
        (Type.CAN_DO, ('reverse',), _pack_req('II7s', 1, 7, b'reverse')),
        (Type.GRAB_JOB, (), _pack_req('II', 9, 0)),
        (Type.PRE_SLEEP, (), _pack_req('II', 4, 0)),
        (Type.SUBMIT_JOB, ('reverse', '', 'test'),
         _pack_req('II8sb4s', 7, 13, b'reverse\0', 0, b'test')),
        (Type.WORK_COMPLETE, ('H:lap:1', 'tset'),
         _pack_req('II8s4s', 13, 12, b'H:lap:1\0', b'tset')),
    ])
    def test_request(self, packet_type, args, expected):
        self.given_protocol_params()
        data = self.protocol.serialize_request(packet_type, *args)
        assert data == expected

    @pytest.mark.parametrize('data,expected', [
        (_pack_res('II', 10, 0), NoJob()),
        (_pack_res('II7s', 8, 7, b'H:lap:1'), JobCreated('H:lap:1')),
        (_pack_res('II', 6, 0), Noop()),
        (_pack_res('II8s8s4s', 11, 20, b'H:lap:1\0', b'reverse\0', b'test'),
         JobAssign('H:lap:1', 'reverse', 'test')),
        (_pack_res('II8s4s', 13, 12, b'H:lap:1\0', b'tset'),
         WorkComplete('H:lap:1', 'tset'))
    ])
    @pytest.mark.asyncio
    async def test_response(self, data, expected, event_loop):
        self.given_protocol_params(event_loop)
        await self.when_data_received(data, event_loop)
        self.then_expect_result(expected)

    @pytest.mark.asyncio
    async def test_partial_receive(self, event_loop):
        self.given_protocol_params(event_loop)

        data1 = self.protocol.serialize_response(Type.JOB_ASSIGN, 'handle1', 'reverse1', '')
        data2 = self.protocol.serialize_request(Type.JOB_ASSIGN, 'handle2', 'reverse2', '')
        data = data1 + data2[:5]

        await self.when_data_received(data, event_loop)
        self.then_expect_result(JobAssign('handle1', 'reverse1', ''))

        await self.when_data_received(data2[5:], event_loop)
        self.then_expect_result(JobAssign('handle2', 'reverse2', ''))

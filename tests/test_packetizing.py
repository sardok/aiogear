import unittest
import pytest
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

    @classmethod
    def setup_class(cls):
        cls.protocol = GearmanProtocolMixin()

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
        data = self.protocol.serialize_request(packet_type, *args)
        assert data == expected

    @pytest.mark.parametrize('data,expected_type,expected', [
        (_pack_res('II', 10, 0), Type.NO_JOB, NoJob()),
        (_pack_res('II7s', 8, 7, b'H:lap:1'), Type.JOB_CREATED, JobCreated('H:lap:1')),
        (_pack_res('II', 6, 0), Type.NOOP, Noop()),
        (_pack_res('II8s8s4s', 11, 20, b'H:lap:1\0', b'reverse\0', b'test'),
         Type.JOB_ASSIGN, JobAssign('H:lap:1', 'reverse', 'test')),
        (_pack_res('II8s4s', 13, 12, b'H:lap:1\0', b'tset'),
         Type.WORK_COMPLETE, WorkComplete('H:lap:1', 'tset'))
    ])
    def test_response(self, data, expected_type, expected):
        parsed_type, response = self.protocol.parse(data)
        assert parsed_type == expected_type
        assert response == expected

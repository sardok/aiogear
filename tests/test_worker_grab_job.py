import asyncio

import pytest

from aiogear import PacketType
from aiogear import Worker
from tests.utils import connect_worker
from .utils import run_mock_server


@pytest.mark.asyncio
@pytest.mark.parametrize('grab_type, mock_protocol, expected', [
    (
        PacketType.GRAB_JOB,
        {
            PacketType.PRE_SLEEP: (PacketType.NOOP, ),
            PacketType.GRAB_JOB: (
                    PacketType.JOB_ASSIGN, 'test_handle', 'test_func', 'test_workload'
            ),
        },
        'test_workload'
    ),
    (
        PacketType.GRAB_JOB_UNIQ,
        {
            PacketType.PRE_SLEEP: (PacketType.NOOP,),
            PacketType.GRAB_JOB_UNIQ: (
                    PacketType.JOB_ASSIGN_UNIQ, 'test_handle', 'test_func', 'test_unique', 'test_workload_unique'
            ),
        },
        'test_workload_unique'
    ),
    (
        PacketType.GRAB_JOB_ALL,
        {
            PacketType.PRE_SLEEP: (PacketType.NOOP,),
            PacketType.GRAB_JOB_ALL: (
                    PacketType.JOB_ASSIGN_ALL, 'test_handle', 'test_func', 'test_unique', 'test_reducer', 'test_workload_all'
            ),
        },
        'test_workload_all'
    ),
])
async def test_grab_types(event_loop, unused_tcp_port, grab_type, mock_protocol, expected):
    f = event_loop.create_future()

    def _test_func(job_info):
        nonlocal f
        f.set_result(job_info.workload)

    await run_mock_server(event_loop, unused_tcp_port, messaging=mock_protocol)
    factory = lambda: Worker((_test_func, 'test_func'), loop=event_loop, grab_type=grab_type)
    worker = await connect_worker(event_loop, unused_tcp_port, factory)
    received = await asyncio.wait_for(f, timeout=1)
    assert received == expected
    await worker.shutdown()

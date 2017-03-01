import pytest
import asyncio
from aiogear import PacketType, Worker
from .utils import run_mock_server, connect_worker


@pytest.mark.asyncio
async def test_job_exception(event_loop, unused_tcp_port):
    f = event_loop.create_future()

    def _raise_exception(job_info):
        raise RuntimeError

    def _exception_cb(param):
        nonlocal f
        f.set_result(param)

    mock_protocol = {
        PacketType.PRE_SLEEP: (PacketType.NOOP,),
        PacketType.GRAB_JOB: (
            PacketType.JOB_ASSIGN, 'test_handle', 'test_exception', 'test_workload'
        ),
        PacketType.WORK_EXCEPTION: _exception_cb,
    }
    await run_mock_server(event_loop, unused_tcp_port, messaging=mock_protocol)
    factory = lambda: Worker((_raise_exception, 'test_exception'), loop=event_loop)
    worker = await connect_worker(event_loop, unused_tcp_port, factory)
    received = await asyncio.wait_for(f, timeout=1)
    assert PacketType.WORK_EXCEPTION == received
    await worker.shutdown()


@pytest.mark.asyncio
async def test_job_result(event_loop, unused_tcp_port):
    f = event_loop.create_future()

    def _return_good(job_info):
        return 'good'

    def _cb(param):
        nonlocal f
        f.set_result(param)

    mock_protocol = {
        PacketType.PRE_SLEEP: (PacketType.NOOP,),
        PacketType.GRAB_JOB: (
            PacketType.JOB_ASSIGN, 'test_handle', 'return_good', 'test_workload'
        ),
        PacketType.WORK_COMPLETE: _cb,
    }
    await run_mock_server(event_loop, unused_tcp_port, messaging=mock_protocol)
    factory = lambda: Worker((_return_good, 'return_good'), loop=event_loop)
    worker = await connect_worker(event_loop, unused_tcp_port, factory)
    received = await asyncio.wait_for(f, timeout=1)
    assert PacketType.WORK_COMPLETE == received
    await worker.shutdown()

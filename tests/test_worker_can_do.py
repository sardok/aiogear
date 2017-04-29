import asyncio
import pytest

from aiogear import Worker, PacketType
from .utils import run_mock_server, connect_worker


def _worker(event_loop, timeout=None):
    func = lambda x: x, 'func_test'
    return Worker(func, loop=event_loop, timeout=timeout)


@pytest.mark.asyncio
@pytest.mark.parametrize('pkt,timeout', [
    (PacketType.CAN_DO, None),
    (PacketType.CAN_DO_TIMEOUT, 10)
])
async def test_can_do(event_loop, unused_tcp_port, pkt, timeout):
    f = event_loop.create_future()

    def _test_func(_):
        nonlocal f
        f.set_result(True)

    await run_mock_server(event_loop, unused_tcp_port, messaging={pkt: _test_func})
    connected = await connect_worker(
        event_loop, unused_tcp_port, lambda: _worker(event_loop, timeout))
    received = await asyncio.wait_for(f, timeout=1)
    assert received is True
    await connected.shutdown()

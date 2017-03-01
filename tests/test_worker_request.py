import pytest
from aiogear import PacketType, Worker


@pytest.fixture
def worker():
    w = Worker()
    return w


@pytest.mark.parametrize('param', [10, '10'])
def test_can_do_timeout(worker, param):
    # Expect no exception
    worker.send(PacketType.CAN_DO_TIMEOUT, 'func', param)
    assert 1 == 1

import pytest
from unittest import mock
from aiogear import Worker, PacketType


@pytest.fixture(scope='function')
def worker():
    w = Worker()
    w.get_task = mock.Mock()
    return w


def test_register_function_connected(worker):
    written = None

    def _write(data):
        nonlocal written
        written = data
    trans = mock.Mock()
    trans.write = _write
    worker.connection_made(trans)
    worker.register_function(lambda x: x, 'test_register')
    assert written == worker.serialize_request(PacketType.CAN_DO, 'test_register')


@pytest.mark.xfail(raises=RuntimeError)
def test_register_function_not_connected(worker):
    worker.register_function(lambda x: x)

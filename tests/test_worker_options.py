import pytest
from aiogear import PacketType, Worker


@pytest.mark.xfail(raises=RuntimeError)
def test_invalid_grab_type():
    Worker(grab_type=PacketType.ALL_YOURS)

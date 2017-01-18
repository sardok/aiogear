import pytest
import unittest
from unittest import mock
from functools import wraps
from aiogear.worker import Worker, JobInfo
from aiogear.mixin import GearmanProtocolMixin
from aiogear.packet import Type
from aiogear.response import JobAssign, JobAssignUniq, JobAssignAll


class TestWorker:
    def setup_method(self, method):
        self.proto_mixin = GearmanProtocolMixin()
        self.worker = None
        self.result = None

    def get_worker(self, *funcs, **kw):
        transport = kw.pop('transport', mock.Mock())
        w = Worker(*funcs, **kw)
        w.transport = transport
        return w

    def given_write_handler(self, handler, *a, **kw):
        def wrapper(f):
            @wraps(f)
            def inner(*a, **kw):
                self.result = f(*a, **kw)
                return self.result
            return inner

        self.given_worker_params(*a, **kw)
        self.worker.transport.write = wrapper(handler)

    def given_worker_params(self, *a, **kw):
        if self.worker is None:
            self.worker = self.get_worker(*a, **kw)

    def when_function_executes(self, fn, *a, **kw):
        self.result = fn(*a, **kw)

    def when_data_received(self, *args, **kw):
        fn = self.worker.data_received
        data = self.worker.serialize_response(*args, **kw)
        self.when_function_executes(fn, data)

    def then_expect_result(self, result):
        assert self.result == result

    def test_dynamic_function_register(self):
        func = lambda x: x
        self.given_write_handler(self.proto_mixin.parse)
        self.when_function_executes(self.worker.register_function, func, 'test_func')
        self.then_expect_result((Type.CAN_DO, b'test_func'))
        assert self.worker.functions.get('test_func') == func

    def test_dynamic_function_register2(self):
        worker = self.get_worker(transport='')
        with pytest.raises(RuntimeError):
            worker.register_function('test')

    def test_auto_function_register(self):
        tr = mock.Mock()
        res = []

        def _write(x):
            res.append(self.proto_mixin.parse(x))

        tr.write = _write
        self.given_worker_params((None, 'func1'), (None, 'func2'), transport='')
        self.when_function_executes(self.worker.connection_made, tr)
        assert res == [(Type.CAN_DO, b'func1'), (Type.CAN_DO, b'func2')]

    @unittest.skip
    def test_grab_job(self):
        grabbed = False

        def func1(_):
            nonlocal grabbed
            grabbed = True

        self.given_worker_params(func1)
        self.when_data_received(Type.JOB_ASSIGN, 'handle', 'func1', '')
        assert grabbed

    def test_job_assign_to_job_info(self):
        self.given_worker_params()
        self.when_function_executes(
            self.worker._to_job_info, JobAssign('test_handle', 'func', 'test\0data'))
        self.then_expect_result(JobInfo('test_handle', 'func', None, None, 'test\0data'))

    def test_job_assign_uniq_to_job_info(self):
        self.given_worker_params()
        self.when_function_executes(
            self.worker._to_job_info, JobAssignUniq('test_handle', 'func', 'unique1', 'test\0data'))
        self.then_expect_result(JobInfo('test_handle', 'func', 'unique1', None, 'test\0data'))

    def test_job_assign_all_to_job_info(self):
        self.given_worker_params()
        self.when_function_executes(
            self.worker._to_job_info,
            JobAssignAll('test_handle', 'func', 'unique2', 'reducer', 'test\0data'))
        self.then_expect_result(JobInfo('test_handle', 'func', 'unique2', 'reducer', 'test\0data'))

    def test_can_do(self):
        self.given_write_handler(self.proto_mixin.parse)
        self.when_function_executes(self.worker.can_do, 'test1')
        self.then_expect_result((Type.CAN_DO, b'test1'))

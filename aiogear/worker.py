import asyncio
import logging
from collections import namedtuple, OrderedDict
from aiogear.packet import Type
from aiogear.mixin import GearmanProtocolMixin
from aiogear.response import NoJob

logger = logging.getLogger(__name__)


JobInfo = namedtuple('JobInfo', ['handle', 'function', 'uuid', 'reducer', 'workload'])


class Worker(GearmanProtocolMixin, asyncio.Protocol):
    def __init__(self, *functions, loop=None, interval=1):
        super(Worker, self).__init__(loop=loop)
        self.transport = None
        self.interval = interval
        self.tasks = []
        self.functions = OrderedDict()
        self.task_no = 1
        self.waiters = []
        for func_arg in functions:
            try:
                func, name = func_arg
                self.functions[name] = func
            except TypeError:
                name = func_arg.__name__
                self.functions[name] = func_arg

    def connection_made(self, transport):
        self.transport = transport

        for fname in self.functions.keys():
            logger.debug('Registering function %s', fname)
            self.can_do(fname)
        self.tasks = [asyncio.Task(self.run()) for _ in range(self.task_no)]

    async def run(self):
        no_job = NoJob()
        while True:
            self.pre_sleep()
            await self.register_response(Type.NOOP)
            response = await self.grab_job()
            if response == no_job:
                continue

            try:
                job_info = self._to_job_info(response)
                func = self.functions.get(job_info.function)
                if not func:
                    logger.warning(
                        'Failed to find function %r in %r', job_info.function,
                        self.functions.keys())
                    self.work_fail(job_info.handle)
                    continue

                try:
                    result_or_coro = func(job_info)
                    if asyncio.iscoroutine(result_or_coro):
                        result = await result_or_coro
                    else:
                        result = result_or_coro
                    self.work_complete(job_info.handle, result)
                except Exception as ex:
                    self.work_exception(job_info.handle, str(ex))

            except AttributeError:
                logger.error('Unexpected GRAB_JOB response %r', response)

    def _to_job_info(self, job_assign):
        attrs = ['handle', 'function', 'uuid', 'reducer', 'workload']
        values = [getattr(job_assign, attr, None) for attr in attrs]
        return JobInfo(*values)

    def _grab_any_job(self):
        return self.grab_job()

    def register_function(self, func, name=''):
        if not self.transport:
            raise RuntimeError('Worker must be connected to the daemon')
        name = name or func.__name__
        self.functions[name] = func
        return self.can_do(name)

    def grab_job_all(self):
        self.send(Type.GRAB_JOB_ALL)
        return self.register_response(Type.NO_JOB, Type.JOB_ASSIGN_ALL)

    def grab_job_uniq(self):
        self.send(Type.GRAB_JOB_UNIQ)
        return self.register_response(Type.NO_JOB, Type.JOB_ASSIGN_UNIQ)

    def grab_job(self):
        self.send(Type.GRAB_JOB)
        return self.register_response(Type.NO_JOB, Type.JOB_ASSIGN)

    def pre_sleep(self):
        self.send(Type.PRE_SLEEP)

    def can_do(self, function):
        self.send(Type.CAN_DO, function)

    def work_fail(self, handle):
        self.send(Type.WORK_FAIL, handle)

    def work_exception(self, handle, data):
        self.send(Type.WORK_EXCEPTION, handle, data)

    def work_complete(self, handle, result):
        if result is None:
            result = ''
        self.send(Type.WORK_COMPLETE, handle, result)

    def set_client_id(self, cid):
        self.send(Type.SET_CLIENT_ID, cid)

import asyncio
import logging
import uuid
import random
from functools import partial
from aiogear.packet import Type
from aiogear.mixin import GearmanProtocolMixin

logger = logging.getLogger(__name__)


class Client(GearmanProtocolMixin, asyncio.Protocol):
    def __init__(self, loop=None):
        super(Client, self).__init__(loop=loop)
        self.transport = None

        self.submit_job = partial(self._submit_job, Type.SUBMIT_JOB)
        self.submit_job_bg = partial(self._submit_job, Type.SUBMIT_JOB_BG)
        self.submit_job_high = partial(self._submit_job, Type.SUBMIT_JOB_HIGH)
        self.submit_job_high_bg = partial(self._submit_job, Type.SUBMIT_JOB_HIGH_BG)
        self.submit_job_low = partial(self._submit_job, Type.SUBMIT_JOB_LOW)
        self.submit_job_low_bg = partial(self._submit_job, Type.SUBMIT_JOB_LOW_BG)

        self.handles = {}

    @staticmethod
    def uuid():
        # 0x00 is used as delimiter in gearman protocol
        # replace it with random printable character.
        replacement = chr(random.randint(32, 126)).encode('ascii')
        return uuid.uuid4().bytes.replace(b'\0', replacement)

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport = None

    async def _submit_job(self, packet, name, *args, **kwargs):
        uuid = kwargs.pop('uuid', None)
        if uuid is None:
            uuid = self.uuid()
        self.send(packet, name, uuid, *args)
        job_created = await self.wait_for(Type.JOB_CREATED)
        f = self.wait_for(Type.WORK_COMPLETE, Type.WORK_FAIL, Type.WORK_EXCEPTION)
        handle = job_created.handle
        self.handles[handle] = f
        f.add_done_callback(lambda _: self.handles.pop(handle))
        return job_created

    def submit_job_sched(self, name, dt, *args, **kwargs):
        sched_args = [str(int(x)) for x in dt.strftime('%M %H %d %m %w').split()]
        return self._submit_job(Type.SUBMIT_JOB_SCHED, name, *(sched_args + list(args)), **kwargs)

    def get_status(self, handle):
        self.send(Type.GET_STATUS, handle)
        return self.wait_for(Type.STATUS_RES)

    def get_status_unique(self, uuid):
        self.send(Type.STATUS_RES_UNIQUE, uuid)
        return self.wait_for(Type.STATUS_RES_UNIQUE)

    def option_req(self, option):
        self.send(Type.OPTION_REQ, option)
        return self.wait_for(Type.OPTION_RES, Type.ERROR)

    def wait_job(self, handle):
        f = self.handles.get(handle)
        if not f:
            raise RuntimeError('Unable to find handle {} in handles {}'.format(handle, self.handles))
        return f

    def set_client_id(self, client_id):
        self.send(Type.SET_CLIENT_ID, client_id)

    def disconnect(self):
        self.transport.close()

import asyncio
import collections
from functools import partial
from . import mixin
from . import packet


PACKET_TYPES = packet.Type


class CallbackClient(mixin.GearmanProtocolMixin, asyncio.Protocol):
    """
    Simple class for submitting a group of foreground jobs and handing updates via
    callbacks rather than explicitly requesting updates.
    Should be instantiated once for each job group.

    Public functions are
    - set_update_callback: set a function to be called as updates come from Gearman
    - submit_jobs: send jobs to the Gearman server
    """
    
    def __init__(self, loop=None):
        super().__init__(loop=loop)
        self.transport = None
        self.submit_job = partial(self._submit_job, PACKET_TYPES.SUBMIT_JOB)
        self.submit_job_high = partial(self._submit_job, PACKET_TYPES.SUBMIT_JOB_HIGH)
        self.submit_job_low = partial(self._submit_job, PACKET_TYPES.SUBMIT_JOB_LOW)
        self.update_callback = lambda *args: None
        self.pending_handles = collections.defaultdict(list)
        self.handles_to_job = {}
        self.priority_map = {
            PACKET_TYPES.SUBMIT_JOB_HIGH: self.submit_job_high,
            PACKET_TYPES.SUBMIT_JOB_LOW: self.submit_job_low,
            PACKET_TYPES.SUBMIT_JOB: self.submit_job
        }

    def set_update_callback(self, async_callback):
        """
        Sets a function to be called with job data updates.
        Function should take the arguments event_type, job_uuid, event_data

        Event types are as follows:

        - progress: a dict containing the keys complete and total
        - data: contains the data returned from Gearman
        - warning: contains the data returned from Gearman
        - complete: contains the string complete, fail, exception depending on how the job finished

        :param async_callback: The async function to be called
        """
        self.update_callback = async_callback

    async def submit_jobs(self, worker_name, jobs):
        """
        Submits a group of jobs using the same worker to Gearman.
        :param worker_name: The Gearman function name
        :param jobs: A list tuples of uuid, data, priority
               Priority is optional and should be one of
               packet.Type.SUBMIT_JOB, packet.Type.SUBMIT_JOB_HIGH, packet.Type.SUBMIT_JOB_LOW
        :return: A future resolved when jobs are accepted, a future resolved when jobs are completed
        """

        self.job_count = len(jobs)

        self.accepted_count = 0
        self.completed_results = []
        self.accepted_future = self.loop.create_future()
        self.complete_future = self.loop.create_future()

        self.do_register(self.jobs_accepted, PACKET_TYPES.JOB_CREATED)
        self.do_register(self.jobs_completed, PACKET_TYPES.WORK_COMPLETE, PACKET_TYPES.WORK_FAIL, PACKET_TYPES.WORK_EXCEPTION)
        self.do_register(self.job_data, PACKET_TYPES.WORK_DATA)
        self.do_register(self.job_warning, PACKET_TYPES.WORK_WARNING)
        self.do_register(self.job_status, PACKET_TYPES.WORK_STATUS)

        self.notify('progress', None, {
            'complete': 0,
            'total': len(jobs)
        })

        async def submit_each(uuid, data, priority=PACKET_TYPES.SUBMIT_JOB):
            """
            Submit each job and wait for the handle to
            come back in job_accepted()
            :param uuid: Globally unique identifier for the job
            :param data: Data to pass to generic_worker
            :param priority: What order should the job be taken out of
             the gearman queue
            :return: Future resolved when all jobs accepted, completed
            Completed future contains a generator of job results
            """

            try:
                submit_funct = self.priority_map[priority]
            except KeyError:
                raise Exception("Unsupported priority {}".format(priority))

            self.ready_send_next = self.loop.create_future()
            await submit_funct(worker_name, data, uuid=uuid)
            handle = await self.ready_send_next
            self.handles_to_job[handle] = uuid

            if handle in self.pending_handles:
                for args in self.pending_handles[handle]:
                    self.notify(*args)
                    self.pending_handles[handle] = None

        for params in jobs:
            await submit_each(*params)

        return self.accepted_future, self.complete_future

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.transport = None
        if self._closing:
            self._closing.set_result(exc)

    def notify(self, event_type, handler, data):

        if handler:
            uuid = self.handles_to_job.get(handler)
            if not uuid:
                self.pending_handles[handler].append((event_type, handler, data))
                return
        else:
            uuid = None

        self.loop.create_task(self.update_callback(event_type, uuid, data))

    def jobs_accepted(self, *args):
        self.accepted_count += 1
        handle = args[0][1][0]
        self.ready_send_next.set_result(handle)

        if self.accepted_count == self.job_count:
            self.accepted_future.set_result(True)

    def jobs_completed(self, *args):

        self.completed_results.append((
            args[0][0].name.split('_')[-1],
            args[-1][1][1]
        ))

        self.notify('progress', None, {
            'complete': len(self.completed_results),
            'total': self.job_count
        })

        self.notify('complete', args[0][1][0], args[0][0].name)

        if len(self.completed_results) == self.job_count:
            self.complete_future.set_result(self.get_results())

    def get_results(self):
        """
        Gets the results of a completed job.
        :return: A generator of dicts with state string and a dictionary of job data
        """
        return ({'state': state, 'data': data} for state, data in self.completed_results)

    def job_data(self, packet):
        noti_type, data = packet
        handle, output = data
        self.notify('data', handle, output)

    def job_warning(self, packet):
        handle, output = map(lambda i: i.decode('ascii'), self._split(packet[1]))
        self.notify('warning', handle, output)

    def job_status(self, packet):
        parts = self._split(packet[1])
        self.notify('status', parts[0].decode('ascii'), {
            'complete': int(parts[1]),
            'total': int(parts[2])
        })

    async def _submit_job(self, packet, name, data, uuid=None):
        self.send(packet, name, uuid, data)
        return uuid

    def get_registered(self, packet):

        def multi_cb(*args):
            output = []
            for key, cb in self._registers:
                if packet in key:
                    output.append(cb(args))

            return output

        return multi_cb

    def disconnect(self):
        self.transport.close()
        f = self._closing = self.loop.create_future()
        return f

    close = disconnect

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

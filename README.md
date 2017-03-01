# aiogear
Asynchronous gearman library based on asyncio implemented in pure python.


## Asynchonous Worker

Once the worker gets connected to the gearman daemon, it registers all the given functions.

```python
async def sleep(job_info):
    sleep_time = int(job_info.workload)
    print(job_info)
    print('Sleeping for ', sleep_time, ' seconds')
    await asyncio.sleep(sleep_time)
    print('Sleep is done')


async def connect_worker(loop, addr, port):
    factory = lambda: Worker(sleep, loop=loop)
    _, worker = await loop.create_connection(factory, addr, port)
    return worker
```

When necessary, register on runtime (with optional custom name) is possible too.

```python
worker.register_function(_sleep, 'sleep')
```

Each function assigned to a particular job, takes `JobInfo` a custom data structure as only parameter regardless of the job type. E.g.:

```python
JobInfo(handle='H:ev-ubuntu:38', function='sleep', uuid=None, reducer=None, workload='5')
```

However, partial functions could be used for extra argument(s).

```python
async def sleep(loop, job_info):
    pass

async def connect_worker(loop, addr, port):
    func = functools.partial(sleep, loop)
    factory = lambda: Worker((func, 'sleep'), loop=loop)
    _, worker = await loop.create_connection(factory, addr, port)
    return worker
```

For running more than one worker in parallel see `examples/` directory.


## Asynchonous Client

As simple as regular tcp client.

```python
async def connect(loop, addr, port):
    client = Client()
    await loop.create_connection(lambda: client, addr, port)
    return client
```

Submitted job returns `JobCreated` a custom data structure with a `handle` property.

```python
job_created = await client.submit_job('sleep', '5')
print(job_created) # JobCreated(handle='H:ev-ubuntu:38')
```

`asyncio.Future` returning `wait_for` method could be for the sake of the task.

```python
def job_is_complete(job_created, f):
    print('Created Job ', job_created, ' is finished')

f = client.wait_job(job_created.handle)
f.add_done_callback(partial(job_is_complete, job_created))
await f
```


For more and complete examples, please see `examples/` directory.

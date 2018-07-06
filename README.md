# aiogear
Asynchronous gearman library based on asyncio implemented in pure python.


## Asynchonous Worker

### Function Registration as Worker

#### Static Registration

Once the worker gets connected to the gearman daemon, it registers all the given functions. In the example below, the only registered function is `sleep`.

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
#### Dynamic Registration

When necessary, registration on runtime (with optional custom name) is possible too.

```python
worker.register_function(_sleep, 'sleep')
```

### Registered Function Calls

Each function assigned to a particular job must accept a sole argument `JobInfo`. It is a custom data structure which represents the job. E.g.:

```python
JobInfo(handle='H:ev-ubuntu:38', function='sleep', uuid=None, reducer=None, workload='5')
```

If needed, partial functions could be used for extra argument(s). The function, passed to `Worker` could be `(function, function_name)` pair.

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

Creating client is as simple as creating tcp client.

```python
async def connect(loop, addr, port):
    client = Client()
    await loop.create_connection(lambda: client, addr, port)
    return client
```

Submitted job returns custom data structure `JobCreated` with a `handle` property which could be used to trace the job.

```python
job_created = await client.submit_job('sleep', '5')
print(job_created) # JobCreated(handle='H:ev-ubuntu:38')
```

`asyncio.Future` returning `wait_job` method could be used to asynchronously block the current context until job is finished.

```python
def job_is_complete(job_created, f):
    print('Created Job ', job_created, ' is finished')

f = client.wait_job(job_created.handle)
f.add_done_callback(partial(job_is_complete, job_created))
await f
```


For more and complete examples, please see `examples/` directory.

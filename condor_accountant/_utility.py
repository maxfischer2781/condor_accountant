from typing import Optional, Callable, TypeVar, Awaitable, AsyncIterable
import asyncio
import subprocess
import os
import collections

from .constants import IP


R = TypeVar("R")


async def run_query(
    *args: bytes,
    ip: IP = IP.ANY,
    pool: Optional[bytes] = None,
) -> asyncio.subprocess.Process:
    """Launch process to run a query using an HTCondor CLI tool"""
    extra_args = [b"-pool", pool] if pool is not None else []
    return await asyncio.create_subprocess_exec(
        args[0],
        *extra_args,
        *args[1:],
        stdin=subprocess.PIPE,
        stdout=subprocess.STDOUT,
        env={**os.environ, **ip.config_env}
    )


class TaskPool:
    def __init__(self, max_size=os.cpu_count()*16):
        self._max_size = max_size
        self._concurrency = asyncio.Semaphore(max_size)

    async def run(self, task: Callable[..., Awaitable[R]], *args, **kwargs) -> R:
        with self._concurrency:
            return await task(*args, **kwargs)

    async def map(
        self, __task: Callable[..., Awaitable[R]], *arg_iters, **kwargs
    ) -> AsyncIterable[R]:
        arguments = zip(*arg_iters)
        task_queue = collections.deque(
            asyncio.ensure_future(self.run(__task, *args, **kwargs))
            for args, _
            in zip(arguments, range(self._max_size))
        )
        try:
            for next_task in task_queue:
                yield await next_task
                task_queue.append(
                    asyncio.ensure_future(self.run(__task, *next(arguments), **kwargs))
                )
        except StopIteration:
            pass
        for next_task in task_queue:
            yield await next_task

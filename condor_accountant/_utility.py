from typing import Optional, Callable, TypeVar, Awaitable, AsyncIterable
import asyncio
import subprocess
import os
import collections
import sys
import inspect

import asyncstdlib as a

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
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
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
        arguments = a.zip(*arg_iters)
        task_queue = collections.deque(
            asyncio.ensure_future(self.run(__task, *args, **kwargs))
            async for args, _
            in a.islice(a.borrow(arguments), self._max_size)
        )
        try:
            for next_task in task_queue:
                yield await next_task
                task_queue.append(
                    asyncio.ensure_future(
                        self.run(__task, *(await a.anext(arguments)), **kwargs)
                    )
                )
        except StopAsyncIteration:
            pass
        for next_task in task_queue:
            yield await next_task


if sys.version_info >= (3, 7):
    asyncio_run = asyncio.run
else:
    # almost literal backport of asyncio.run
    def asyncio_run(main, *, debug=None):
        assert inspect.iscoroutine(main)
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            if debug is not None:
                loop.set_debug(debug)
            return loop.run_until_complete(main)
        finally:
            try:
                _cancel_all_tasks(loop)
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.set_event_loop(None)
                loop.close()

    def _cancel_all_tasks(loop):
        to_cancel = asyncio.Task.all_tasks(loop)
        if not to_cancel:
            return
        for task in to_cancel:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*to_cancel, return_exceptions=True))
        for task in to_cancel:
            if task.cancelled():
                continue
            if task.exception() is not None:
                loop.call_exception_handler(
                    {
                        "message": "unhandled exception during asyncio.run() shutdown",
                        "exception": task.exception(),
                        "task": task,
                    }
                )

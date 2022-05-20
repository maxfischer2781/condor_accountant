from typing import Optional, Callable, TypeVar, Awaitable, AsyncIterable, AsyncGenerator
import time
import asyncio
import subprocess
import os
import collections
import sys
import inspect

import asyncstdlib as a

from .constants import IP


R = TypeVar("R")


DEBUG_QUERIES = os.environ.get("_ConAcc_DEBUG_QUERIES", "").strip()


@a.contextmanager
async def run_query(
    *args: bytes,
    ip: IP = IP.ANY,
    pool: Optional[bytes] = None,
) -> AsyncGenerator[asyncio.subprocess.Process, None]:
    """Launch process to run a query using an HTCondor CLI tool"""
    if pool is not None:
        args = [args[0], b"-pool", pool, *args[1:]]
    if DEBUG_QUERIES.lower() == "true" or DEBUG_QUERIES.encode() == args[0]:
        print(args, file=sys.stderr)
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={**os.environ, **ip.config_env},
    )
    try:
        yield process
    finally:
        # kill process then wait until it has finished
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await process.communicate()


# 'warnings.warn' gets reset whenever we start a subprocess
# so let's reinvent the wheel...
def debug(*messages, sep=" ", once=True, _registry=set()):
    """Emit a debug message"""
    message = sep.join(map(str, messages))
    if once and message in _registry:
        return
    _registry.add(message)
    print(message, file=sys.stderr)


class Throttle:
    """
    Awaitable that limits how often it lets waiters pass

    :param delay: minimum delay between two waiters proceeding
    """
    __slots__ = ("delay", "_next", "_lock")

    def __init__(self, delay: float):
        self.delay = delay
        self._next = 0
        self._lock = asyncio.Lock()

    def __await__(self):
        if self.delay == 0:
            return
        yield from self._lock.acquire()
        try:
            now = time.monotonic()
            if now < self._next:
                yield from asyncio.sleep(self._next - now)
                self._next += self.delay
            else:
                self._next = now + self.delay
        finally:
            self._lock.release()

    async def __aenter__(self):
        await self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class TaskPool:
    """
    Pool of concurrency to run only a bounded number of tasks at once

    :param max_size: upper limit on concurrently running tasks
    :param throttle: minimum delay between tasks starting
    """

    def __init__(self, max_size=os.cpu_count() * 16, throttle=0.0):
        assert max_size > 0
        self._max_size = max_size
        self._throttle = throttle
        self._concurrency = asyncio.Semaphore(max_size)
        self._delay = Throttle(throttle)

    async def run(self, __task: Callable[..., Awaitable[R]], *args, **kwargs) -> R:
        """Directly run a single task subject to concurrency and throttle limits"""
        async with self._concurrency, self._delay:
            return await __task(*args, **kwargs)

    def submit(
        self, __task: Callable[..., Awaitable[R]], *args, **kwargs
    ) -> "asyncio.Future[R]":
        """
        Concurrently run a single task subject to concurrency and throttle limits
        """
        return asyncio.ensure_future(self.run(__task, *args, **kwargs))

    async def map(
        self, __task: Callable[..., Awaitable[R]], *arg_iters, **kwargs
    ) -> AsyncIterable[R]:
        """
        Like :py:func:`map` but running multiple tasks concurrently

        Returns an async iterator that awaits the function for every argument tuple
        from `arg_iters`, yielding the results in-order. When `kwargs` are given,
        they are passed completely to each invocation of the function.
        """
        arguments = a.zip(*arg_iters)
        task_queue = collections.deque()
        async for args in a.islice(a.borrow(arguments), self._max_size):
            task_queue.append(asyncio.ensure_future(self.run(__task, *args, **kwargs)))
        try:
            # the task_queue cannot be empty since we add a new task for each one done
            yield await task_queue.popleft()
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

import sys
from typing import Collection, Optional
import asyncio
import re

from ..constants import Subsystem, AccessLevel, IP
from .._infosystem.nodes import Node
from .._utility import run_query, TaskPool


async def ping_nodes(
    subsystem: Subsystem,
    *levels: AccessLevel,
    timeout: Optional[float] = None,
    ip: IP = IP.ANY,
    pool: Optional[bytes] = None
) -> "dict[str | AccessLevel, list[Node]]":
    """Ping all nodes of a given `subsystem` type, checking access `levels`"""
    nodes = Node.from_pool(subsystem)
    queries = TaskPool(throttle=1/100).map(
        _check_connectivity, nodes, levels=levels, timeout=timeout, ip=ip, pool=pool
    )
    failures = {}
    async for node, connected, accepted in queries:
        if not connected:
            failures.setdefault("connect", []).append(node)
        else:
            for level in set(levels) - accepted:
                failures.setdefault(level, []).append(node)
    return failures


FAIL_CONNECT = b"ERROR: failed to make connection to"
SUCCESS_PATTERN = re.compile(rb"^(\w*) command using \(.*\) succeeded as (.*) to .*\.")
FAIL_PATTERN = re.compile(rb"^(\w*) failed!")


async def _check_connectivity(
    node: Node,
    levels: Collection[AccessLevel],
    timeout: float,
    ip: IP = IP.ANY,
    pool: Optional[bytes] = None,
) -> "tuple[Node, bool, set[AccessLevel]]":
    try:
        accesses = await asyncio.wait_for(
            _ping_host(node.name, levels, subsystem=node.type, ip=ip, pool=pool),
            timeout,
        )
    except (ConnectionError, asyncio.TimeoutError):
        return node, False, set()
    else:
        return node, True, {
            level for level, identity in accesses.items() if identity is not None
        }


async def _ping_host(
    name: bytes,
    levels: Collection[AccessLevel],
    subsystem: Subsystem = Subsystem.MASTER,
    ip: IP = IP.ANY,
    pool: Optional[bytes] = None,
) -> "dict[AccessLevel, Optional[bytes]]":
    """
    Perform `condor_ping` and return the identity used for each access `level`

    If authentication fails for a specific level, its identity is :py:data:`None`.
    If the host cannot be reached, :py:exc:`ConnectionError` is raised.
    """
    if not levels:
        return {}
    authentications = {}
    async with run_query(
        b"condor_ping",
        *[b"-type", subsystem.name.encode(), b"-name", name.replace(b'"', rb'\"')],
        *(level.name.encode() for level in levels),
        ip=ip,
        pool=pool,
    ) as condor_ping:
        async for line in condor_ping.stdout:
            if line.startswith(FAIL_CONNECT):
                raise ConnectionError
            try:
                level, identity = SUCCESS_PATTERN.match(line).groups()
            except AttributeError:
                try:
                    level, identity = FAIL_PATTERN.match(line)[1], None
                except TypeError:
                    print("failed to parse condor_ping", line, file=sys.stderr)
                    continue
            authentications[AccessLevel[level.decode()]] = identity
    return authentications

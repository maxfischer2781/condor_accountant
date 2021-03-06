from typing import Collection, Optional, TypeVar
import asyncio
import re

from ..constants import Subsystem, AccessLevel, IP
from .._infosystem.nodes import Node
from .._utility import run_query, TaskPool, debug


K = TypeVar("K")
V = TypeVar("V")


def _remove_empty(items: "dict[K, V]") -> "dict[K, V]":
    return {key: value for key, value in items.items() if value}


async def ping_nodes(
    subsystem: Subsystem,
    *levels: AccessLevel,
    timeout: Optional[float] = None,
    ip: IP = IP.ANY,
    pool: Optional[bytes] = None
) -> "tuple[dict[AccessLevel, set[Node]], dict[str | AccessLevel, set[Node]]]":
    """Ping all nodes of a given `subsystem` type, checking access `levels`"""
    nodes = Node.from_pool(subsystem)
    queries = TaskPool(max_size=64, throttle=1 / 128).map(
        _check_connectivity, nodes, levels=levels, timeout=timeout, ip=ip, pool=pool
    )
    successes = {level: set() for level in levels}
    failures = {"connect": set(), **{level: set() for level in levels}}
    async for node, identities in queries:
        if not identities:
            failures["connect"].add(node)
        else:
            for level, identity in identities.items():
                if identity is None:
                    failures[level].add(node)
                else:
                    successes[level].add(node)
    return _remove_empty(successes), _remove_empty(failures)


FAIL_CONNECT = b"ERROR: failed to make connection to"
SUCCESS_PATTERN = re.compile(rb"^(\w*) command using \(.*\) succeeded as (.*) to .*\.")
FAIL_PATTERN = re.compile(rb"^(\w*) failed!")


async def _check_connectivity(
    node: Node,
    levels: Collection[AccessLevel],
    timeout: float,
    ip: IP = IP.ANY,
    pool: Optional[bytes] = None,
) -> "tuple[Node, dict[AccessLevel, Optional[bytes]]]":
    try:
        accesses = await asyncio.wait_for(
            _condor_ping(node.address, levels, ip=ip, pool=pool), timeout=timeout
        )
    except (ConnectionError, asyncio.TimeoutError):
        return node, {}
    else:
        return node, accesses


async def _condor_ping(
    address: bytes,
    levels: Collection[AccessLevel],
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
        b"-address",
        address,
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
                    debug("'condor_ping' output unexpected:", repr(line))
                    continue
            authentications[AccessLevel[level.decode()]] = identity
    return authentications

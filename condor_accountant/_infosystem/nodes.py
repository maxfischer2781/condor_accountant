"""
Find and inspect nodes/daemon
"""
from typing import NamedTuple, Optional, Iterator
import asyncstdlib as a

from .._utility import run_query
from ..constants import Subsystem


class Node(NamedTuple):
    name: bytes      # Name
    machine: bytes   # Machine
    type: Subsystem  # MyType
    address: bytes   # MyAddress

    @classmethod
    async def from_pool(
        cls, __type: Subsystem, pool: Optional[bytes] = None
    ) -> "Iterator[Node]":
        """Query the `pool` for all nodes of a specific type"""
        condor_status = await run_query(
            *[b"condor_status", b"-subsystem", __type.name.lower().encode()],
            *[b"-format", b"%s\t", b"Name", b"-format", b"%s\t", b"Machine"],
            *[b"-format", b"%s\n", b"MyAddress"],
            pool=pool,
        )
        async for line in a.map(bytes.strip, condor_status.stdout):
            if not line:
                continue
            name, machine, address = line.split(b"\t")
            yield cls(name, machine, __type, address)

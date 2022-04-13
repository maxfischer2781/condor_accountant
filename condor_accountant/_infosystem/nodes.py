"""
Find and inspect nodes/daemon
"""
from typing import NamedTuple, Optional, Iterator
import asyncstdlib as a

from .._utility import run_query
from ..constants import Subsystem


class Node(NamedTuple):
    """Representation of an HTCondor node of a specific type"""
    # Name
    name: bytes
    # Machine
    machine: bytes
    type: Subsystem
    # MyAddress
    address: bytes

    @classmethod
    async def from_pool(
        cls, __type: Subsystem, pool: Optional[bytes] = None
    ) -> "Iterator[Node]":
        """Query the `pool` for all nodes of a specific type"""
        constraints = (
            []
            if __type != Subsystem.STARTD
            else [b"-constraint", b'SlotType=!="Dynamic"']
        )
        async with run_query(
            *[b"condor_status", b"-subsystem", __type.name.lower().encode()],
            *[b"-format", b"%s\t", b"Name", b"-format", b"%s\t", b"Machine"],
            *[b"-format", b"%s\n", b"MyAddress"],
            *constraints,
            pool=pool,
        ) as condor_status:
            async for line in a.map(bytes.strip, condor_status.stdout):
                if not line:
                    continue
                name, machine, address = line.split(b"\t")
                yield cls(name, machine, __type, address)

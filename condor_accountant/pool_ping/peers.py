"""
Test the connectivity to other nodes in the pool which are of interest to this node
"""
from typing import Optional

from .connectivity import ping_nodes
from ..constants import Subsystem, AccessLevel, IP
from .._infosystem import configuration
from .._utility import asyncio_run

# https://htcondor.readthedocs.io/en/lts/admin-manual/security.html#access-level-descriptions
DAEMON_PEERS = {
    Subsystem.MASTER: {
        Subsystem.COLLECTOR: {AccessLevel.ADVERTISE_MASTER},
    },
    Subsystem.SCHEDD: {
        Subsystem.STARTD: {AccessLevel.WRITE},
        Subsystem.NEGOTIATOR: {AccessLevel.WRITE},
        Subsystem.COLLECTOR: {AccessLevel.ADVERTISE_SCHEDD},
    },
    Subsystem.STARTD: {
        Subsystem.SCHEDD: {AccessLevel.READ},
        Subsystem.COLLECTOR: {AccessLevel.ADVERTISE_STARTD},
    },
    Subsystem.NEGOTIATOR: {
        Subsystem.SCHEDD: {AccessLevel.NEGOTIATOR},
        Subsystem.STARTD: {AccessLevel.NEGOTIATOR},
        Subsystem.COLLECTOR: {AccessLevel.READ, AccessLevel.DAEMON},
    },
}


async def required_peers(
    config_root: Optional[bytes] = None,
) -> "dict[Subsystem, set[AccessLevel]]":
    """Get the peers and their access levels of the local HTCondor node"""
    peers = {}
    for local_daemon in await configuration.daemons(config_root):
        for peer, levels in DAEMON_PEERS.get(local_daemon, {}).items():
            peers.setdefault(peer, set()).update(levels)
    return peers


async def test_peers(timeout: float = 5.0, config_root: Optional[bytes] = None):
    peers = await required_peers(config_root)
    ip_versions = await configuration.ip_versions(config_root)
    for peer, levels in peers.items():
        for ip in ip_versions:
            failures = await ping_nodes(peer, *levels, timeout=timeout, ip=ip)
            if not failures:
                continue
            print("failed pings: subsystem", peer.name, f"[IP{ip.name}]")
            for reason, nodes in failures.items():
                print(
                    f"failed {reason if isinstance(reason, str) else reason.name}",
                    *(node.name.decode(errors="surrogateescape") for node in nodes),
                )


def main():
    asyncio_run(test_peers())


if __name__ == "__main__":
    main()

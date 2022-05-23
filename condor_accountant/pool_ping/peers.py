"""
Test the connectivity to other nodes in the pool which are of interest to this node
"""
from typing import Optional, Any
import json

from .connectivity import ping_nodes
from ..constants import Subsystem, AccessLevel, IP
from .._infosystem import configuration, nodes
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


class PingPeersReport:
    def __init__(
        self,
        ip: IP,
        subsystem: Subsystem,
        successes: "dict[AccessLevel, set[nodes.Node]]",
        failures: "dict[str | AccessLevel, set[nodes.Node]]",
    ):
        self.ip = ip
        self.subsystem = subsystem
        self.successes = successes
        self.failures = failures

    @property
    def human(self) -> str:
        parts = [f"ping_peers to subsystem {self.subsystem.name} [IP{self.ip.name}]"]
        data = self.data
        for key, head in (("successes", "SUCCESS"), ("failures", "FAILURE")):
            if data[key]:
                parts.append(head)
                parts.extend(
                    f"{level}: {','.join(peer_nodes)}"
                    for level, peer_nodes
                    in data[key].items()
                )
        return "\n".join(parts)

    @property
    def json(self) -> str:
        return json.dumps(self.data)

    @property
    def data(self) -> "dict[str, Any]":
        return {
                "test": "ping_peers",
                "ip": self.ip.name,
                "subsystem": self.subsystem.name,
                "successes": {
                    level.name: sorted(node.name for node in peer_nodes)
                    for level, peer_nodes in self.successes.items()
                },
                "failures": {
                    getattr(level, "name", level): sorted(
                        node.name for node in peer_nodes
                    )
                    for level, peer_nodes in self.successes.items()
                },
            }


async def test_peers(
    timeout: float = 8.0, config_root: Optional[bytes] = None
) -> "list[PingPeersReport]":
    """Test and report condor_ping'ing all peers of the current node"""
    peers = await required_peers(config_root)
    ip_versions = await configuration.ip_versions(config_root)
    results = []
    for peer, levels in peers.items():
        for ip in ip_versions:
            successes, failures = await ping_nodes(
                peer, *levels, timeout=timeout, ip=ip
            )
            results.append(PingPeersReport(
                ip=ip, subsystem=peer, successes=successes, failures=failures
            ))
    return results


def main():
    reports = asyncio_run(test_peers())
    for report in reports:
        print(report.human)


if __name__ == "__main__":
    main()

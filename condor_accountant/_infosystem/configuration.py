from typing import Optional
import re
import ipaddress

from .._utility import run_query
from ..constants import Subsystem, IP


LIST_SEP_PATTERN = re.compile(b"[, ]+")


async def daemons(config_root: Optional[bytes] = None) -> "set[Subsystem]":
    """The subsystems (SCHEDD, STARTD, ...) configured to run on this node"""
    daemon_names = LIST_SEP_PATTERN.split(
        (await _query_config(b"DAEMON_LIST", root=config_root)).strip()
    )
    return {Subsystem[name.decode()] for name in daemon_names if name}


async def ip_versions(config_root: Optional[bytes] = None) -> "list[IP]":
    """The IP addresses (V4, V6) configured to be used on this node"""
    ipv4, ipv6 = (await _query_config(b"DAEMON_LIST", root=config_root)).splitlines()
    ips = []
    for kind, configured, Address in (
        (IP.V4, ipv4, ipaddress.IPv4Address), (IP.V6, ipv6, ipaddress.IPv6Address)
    ):
        try:
            address = Address(configured.decode().strip())
        except ipaddress.AddressValueError:
            continue
        else:
            if address.is_loopback or address.is_unspecified or address.is_link_local:
                continue
            ips.append(kind)
    return ips


async def _query_config(*key: bytes, root: Optional[bytes] = None) -> bytes:
    async with run_query(
        b"condor_config_val",
        *key,
        *([b"-root-config", root] if root is not None else []),
    ) as condor_config_val:
        return await condor_config_val.stdout.read()

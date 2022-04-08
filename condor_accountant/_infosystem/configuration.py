from typing import Optional
import re

from .._utility import run_query
from ..constants import Subsystem


LIST_SEP_PATTERN = re.compile(b"[, ]+")


async def daemons(config_root: Optional[bytes] = None) -> "set[Subsystem]":
    daemon_names = LIST_SEP_PATTERN.split(
        (await _query_config(b"DAEMON_LIST", root=config_root)).strip()
    )
    return {Subsystem[name.decode()] for name in daemon_names if name}


async def _query_config(key: bytes, root: Optional[bytes] = None) -> bytes:
    condor_config_val = await run_query(
        *[b"condor_config_val", key],
        *([b"-root-config", root] if root is not None else []),
    )
    return await condor_config_val.stdout.read()

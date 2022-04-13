from typing import Dict
import enum


class AccessLevel(enum.Enum):
    ALL = enum.auto()
    READ = enum.auto()
    WRITE = enum.auto()
    ADMINISTRATOR = enum.auto()
    SOAP = enum.auto()
    CONFIG = enum.auto()
    OWNER = enum.auto()
    DAEMON = enum.auto()
    NEGOTIATOR = enum.auto()
    ADVERTISE_MASTER = enum.auto()
    ADVERTISE_STARTD = enum.auto()
    ADVERTISE_SCHEDD = enum.auto()
    CLIENT = enum.auto()


class Subsystem(enum.Enum):
    C_GAHP = enum.auto()
    C_GAHP_WORKER_THREAD = enum.auto()
    CKPT_SERVER = enum.auto()
    COLLECTOR = enum.auto()
    DBMSD = enum.auto()
    DEFRAG = enum.auto()
    EC2_GAHP = enum.auto()
    GANGLIAD = enum.auto()
    GCE_GAHP = enum.auto()
    GRIDMANAGER = enum.auto()
    HAD = enum.auto()
    JOB_ROUTER = enum.auto()
    KBDD = enum.auto()
    LEASEMANAGER = enum.auto()
    MASTER = enum.auto()
    NEGOTIATOR = enum.auto()
    REPLICATION = enum.auto()
    ROOSTER = enum.auto()
    SCHEDD = enum.auto()
    SHADOW = enum.auto()
    SHARED_PORT = enum.auto()
    STARTD = enum.auto()
    STARTER = enum.auto()
    SUBMIT = enum.auto()
    TOOL = enum.auto()
    TRANSFERER = enum.auto()


class IP(enum.Enum):
    V4 = enum.auto()
    V6 = enum.auto()
    ANY = enum.auto()

    @property
    def config_env(self) -> Dict[str, str]:
        """partial process environment to configure HTCondor with this IP version"""
        if self is IP.ANY:
            return {}
        return {
            f"_CONDOR_ENABLE_IP{kind.name}": "true" if self is kind else "false"
            for kind in (IP.V4, IP.V6)
        }

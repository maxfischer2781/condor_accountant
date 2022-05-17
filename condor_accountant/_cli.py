import argparse
from .pool_ping import peers as ping_peers

CLI = argparse.ArgumentParser()
CLI.add_argument(
    "TOOL",
    choices=["ping-peers"]
)


def main():
    options = CLI.parse_args()
    if options.TOOL == "ping-peers":
        ping_peers.main()

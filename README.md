# HTCondor Accountant – keep tabs on your cluster

This is a very early draft. Don't use. ;)

## Installation and usage

All that is needed is Python3 and HTCondor installed on a node.
You can install the tool directly from github:

```bash
$ # create a venv to isolate the installation
$ python3 -m venv ca_venv
$ ca_venv/bin/python -m pip install --upgrade pip
$ ca_venv/bin/python -m pip install git+https://github.com/maxfischer2781/condor_accountant.git
```

```bash
$ ca_venv/bin/condor_accountant ping-peers
```

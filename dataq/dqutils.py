"Convenience functions for data-queue."

import os, os.path
import logging

def decode_dict(byte_dict):
    "Convert dict containing bytes as keys and values one containing strings."
    str_dict = dict()
    for k, val in list(byte_dict.items()):
        str_dict[k.decode()] = val.decode()
    return str_dict


def save_pid(progpath):
    "Write the PID of this process to a file so we can kill it later."
    base = os.path.basename(progpath)
    piddir = '/var/run/dataq'
    pidfile = os.path.join(piddir, base +'.pid')
    # Following is BAD!!! Platform dependent location of PID file.
    # init functions.daemon would not write pid. This was last resort.
    # What is the right way???
    pid = os.getpid()
    os.makedirs(piddir, mode=0o777, exist_ok=True)
    with open(pidfile, 'w') as fobj:
        print(pid, file=fobj, flush=True)
    os.chmod(pidfile, 0o777)

    return pid


"Convenience functions for data-queue."

import os
import os.path
import logging
import traceback
import socket

def traceback_if_debug():
    "Print traceback of logging level is set to DEBUG"
    if logging.DEBUG == logging.getLogger().getEffectiveLevel():
        traceback.print_exc()

def decode_dict(byte_dict):
    "Convert dict containing bytes as keys and values one containing strings."
    str_dict = dict()
    for k, val in list(byte_dict.items()):
        str_dict[k.decode()] = val.decode()
    return str_dict


def save_pid(progpath, piddir='/var/run/dataq'):
    "Write the PID of this process to a file so we can kill it later."
    base = os.path.basename(progpath)
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

def get_keyword(keyword, kwargs):
    'Used for ACTIONS, which do not all have same signature.'
    if keyword not in kwargs:
        raise Exception('Did not get required keyword parameter: "{}" in: {}'
                        .format(keyword, kwargs))
    return kwargs[keyword]


def push_to_q(dq_host, dq_port, fname, checksum):
    'Push a line onto data-queue named by qname.'
    logging.debug('push_to_q({}, {}, {})'.format(dq_host, dq_port, fname))
    data = '{} {}\n'.format(checksum, fname)
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect to server and send data
        sock.connect((dq_host, dq_port))
        sock.sendall(bytes(data, 'UTF-8'))

        # Receive data from the server and shut down
        received = sock.recv(1024)
    finally:
        sock.close()


# Refactor to use this func where "tail" used in actions.py !!!
def mirror_path(src_root, fname, new_root, new_base=None):
    'Return path in new root constructed from src_root and fname.'
    head = os.path.dirname(fname)
    base = os.path.basename(fname) if new_base == None else new_base
    iname = os.path.join(new_root,
                         os.path.relpath(head, src_root),
                         base)
    return iname


def move(src_root, src_abs_fname, dest_root, dest_basename=None):
    """Rename a subtree under src_abs_fname to one under
dest_root. Dest_basename defaults to base of src_abs_fname."""
    dest_fname = mirror_path(src_root, src_abs_fname, dest_root,
                             new_base=dest_basename)
    logging.debug('move [base={}] {} to {}'
                  .format(dest_basename, src_abs_fname, dest_fname))
    os.makedirs(os.path.dirname(dest_fname), exist_ok=True)
    if src_abs_fname == dest_fname:
        raise Exception('Tried to move filename to itself: {}'
                        .format(dest_fname))
    os.rename(src_abs_fname, dest_fname)
    return dest_fname

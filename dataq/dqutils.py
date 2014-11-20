"Convenience functions for data-queue."

import os, os.path
import logging

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


# !!! Refactor to use this func where "tail" used in actions.py !!!
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
dest_dir. Dest_basename defaults to base of src_abs_fname."""
    #! # changing part of path tail
    #! tail  = os.path.relpath(src_abs_fname, src_root)  
    #! os.makedirs(os.path.join(dest_root, os.path.dirname(tail)),
    #!             exist_ok=True)
    #! logging.debug('dest_root={}, tail={}, base={}'
    #!               .format(dest_root, tail, dest_basename))
    #! fname = os.path.join(dest_root, os.path.dirname(tail), dest_basename)
    #! os.rename(src_abs_fname, fname)

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

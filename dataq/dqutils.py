"Convenience functions for data-queue."

import os
import os.path
import logging
import traceback
import socket

import redis

from .dbvars import *

def trace_str():
    return ''.join(traceback.format_exc())

def traceback_if_debug():
    "Print traceback of logging level is set to DEBUG"
    if logging.DEBUG == logging.getLogger().getEffectiveLevel():
        logging.debug(''.join(traceback.format_exc()))

def log_rid(r, rid, msg):
    'Diagnostic only'
    logging.debug('dbg-{}: {}={}'.format(msg, rid,r.hgetall(rid)))

def redis_vars(r, rid):
    'Diagnostic only'
    prms = dict(rid=rid,
                aq=aq,
                aqs=aqs,
                iq=iq,
                ridval=r.hgetall(rid),
                aqval=r.lrange(aq, 0, 999),
                aqsval=r.smembers(aqs),
                iqval=r.lrange(iq, 0, 999),
                )
    return ('REDIS variables: {rid}={ridval}, {aq}={aqval}, {aqs}={aqsval}, {iq}={iqval}'
            .format(**prms))

        
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


def push_to_q(dq_host, dq_port, fname, checksum, timeout=20):
    'Push a line onto data-queue named by qname.'
    logging.error('dbg-0.0: EXECUTING push_to_q({}, {}, {}, {}, timeout={})'
                  .format(dq_host, dq_port, fname, checksum, timeout))

    data = '{} {}\n'.format(checksum, fname)
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout) # seconds
    try:
        # Connect to server and send data
        sock.connect((dq_host, dq_port))
        sock.sendall(bytes(data, 'utf-8'))

        # Receive data from the server and shut down
        received = str(sock.recv(1024), 'utf-8')
    except:
        raise
    finally:
        sock.close()
    # sent successfully 
    logging.debug('Sent line to  dq-push server: {})'.format(data))
    logging.debug('Received from dq-push server: {})'.format(received))

# May options for StrictRedis.  Some may be useful when more heavily used.
# Alas, they are not well documented.
# e.g.  socket_timeout=None, socket_connect_timeout=None,
# socket_keepalive=None, socket_keepalive_options=None,
# connection_pool=None, charset='utf-8', errors='strict',
# decode_responses=False, retry_on_timeout=False,
# unix_socket_path=None


def push_records(host, port, records, max_qsize):
    'records :: list(dict(filename, checksum))'
    r = redis.StrictRedis(host=host, port=port,
                          socket_keepalive=True,
                          retry_on_timeout=True )
    if r.get(readP) == 'off':
        return False
    

    if r.llen(aq) > max_qsize:
        logging.error('Queue is full! '
                      + 'Turning off read from socket. '
                      + 'Disabling push to queue.  '
                      + 'To reenable: "dqcli --read on"  '
                      )
        r.set(readP, 'off')
        return False
    
    for rec in records:
        checksum = rec['checksum']
        if r.sismember(aqs, checksum) == 1:
            logging.warning(': Record for {} is already in queue.'
                            +' Ignoring duplicate.'.format(checksum))
            continue
        # buffer all commands done by pipeline, make command list atomic
        with r.pipeline() as pl:
            while True: # retry if clients collide on watched variables
                try:
                    pl.watch(rids, aq, aqs, checksum)
                    pl.multi()
                    # add to DB
                    pl.sadd(aqs, checksum)
                    pl.lpush(aq, checksum)
                    pl.sadd(rids, checksum)
                    pl.hmset(checksum, rec)
                    pl.hset(ecnt, checksum, 0) # error count against file
                    # Could put following outside REC loop to save time!!!
                    pl.save()
                    pl.execute()
                    break
                except redis.WatchError as ex:
                    logging.debug('Got redis.WatchError: {}'.format(ex))
                    # another client must have changed  watched vars between
                    # the time we started WATCHing them and the pipeline's
                    # execution. Our best bet is to just retry.
                    continue # while True
        # END: with pipeline
        log_rid(r, checksum, 'end push_records()')
    
def push_direct(redis_host, redis_port, fname, checksum, cfg):
    'Directly push a record to (possibly remote) REDIS'

    r = redis.StrictRedis(host=redis_host, port=redis_port,
                          socket_keepalive=True,
                          retry_on_timeout=True )

    if r.get(readP) == 'off':
        return False

    if r.sismember(aqs, checksum) == 1:
        logging.warning('Record for {} is already in queue. Ignoring duplicate.'
                        .format(checksum))
        return False

    if r.llen(aq) > cfg['maxium_queue_size']:
        logging.error('Queue is full! '
                      + 'Turning off read from socket. '
                      + 'Disabling push to queue.  '
                      + 'To reenable: "dqcli --read on"  '
                      )
        r.set(readP, 'off')
        return False
    
    rec = dict(filename=fname, checksum=checksum)

    # buffer all commands done by pipeline, make command list atomic
    with r.pipeline() as pl:
        while True: # retry if clients collide on watched variables
            try:
                pl.watch(rids, aq, aqs, checksum)
                pl.multi()
                # add to DB
                pl.sadd(aqs, checksum)
                pl.lpush(aq, checksum)
                pl.sadd(rids, checksum)
                pl.hmset(checksum, rec)
                pl.hset(ecnt, checksum, 0) # error count against file
                pl.save()
                pl.execute()
                break
            except redis.WatchError as ex:
                logging.debug('Got redis.WatchError: {}'.format(ex))
                # another client must have changed  watched vars between
                # the time we started WATCHing them and the pipeline's
                # execution. Our best bet is to just retry.
                continue # while True
    # END: with pipeline
    log_rid(r, checksum, 'end push_direct()')
    
# Refactor to use this func where "tail" used in actions.py 
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

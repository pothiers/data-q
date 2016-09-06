"""Interface to working with REDIS.  All of our calls to REDIS
provided functions should be in this file. (but they are not yet!) """

import redis
import logging

from .dbvars import *
from tada import settings

def decode_dict(byte_dict):
    "Convert dict containing bytes as keys and values one containing strings."
    str_dict = dict()
    for k, val in list(byte_dict.items()):
        str_dict[k.decode()] = val.decode()
    return str_dict


def log_rid(r, rid, msg):
    'Diagnostic only'
    logging.debug('DBG-{}: {}={}'.format(msg, rid,r.hgetall(rid)))

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
    return ('REDIS variables: '
            '{rid}={ridval}, {aq}={aqval}, {aqs}={aqsval}, {iq}={iqval}'
            .format(**prms))

        

# There are many options for StrictRedis.  Some may be useful when more
# heavily used. Alas, they are not well documented.
# e.g.  socket_timeout=None, socket_connect_timeout=None,
# socket_keepalive=None, socket_keepalive_options=None,
# connection_pool=None, charset='utf-8', errors='strict',
# decode_responses=False, retry_on_timeout=False,
# unix_socket_path=None

def redis_protocol():
    return redis.StrictRedis()

def action_p(red):
    return red.get(actionP) == b'on'

def remove_active(red, rid):
    red.srem(aqs, rid)

def next_record(red, timeout=60):
    """timeout:: seconds to wait before unblocking (but then start over)"""
    # ALERT: being "clever" here!
    #
    # If actionP was turned on, and the queue isn't being filled,
    # we will eventually pop everything from the queue and block.
    # But then if actionP is turned OFF, we don't know yet because
    # we are still blocking.  So next queue item will sneak by.
    # Probably unimportant on long running system, but plays havoc
    # with testing. To resolve, on setting actionP to off, we push
    # to dummy to clear block.
    #
    
    try:
        #! (keynameB, ridB) = red.brpop([dummy, aq]) # BLOCKING pop (over key list)
        (keynameB, ridB) = red.brpop([dummy, aq], timeout) # BLOCKING pop (over key list)
        if (keynameB.decode() == dummy):
            rid = None
            logging.debug('DG next_record got value ({}) on DUMMY channel'.
                          format(ridB))
        else:
            rid =  ridB.decode()
            red.srem(aqs, rid)
    except:
        logging.debug('Stopped blocking after timeout ({}) exceeded'
                      .format(timeout))
        return None
    return rid



def get_record(red, rid):
    return decode_dict(red.hgetall(rid))

def set_record(red, rid, record):
    red.hmset(rid, record)
    red.hset(ecnt, rid, 0) # error count against file
    red.sadd(rids, rid)

def remove_record(red, rid):
    #!logging.debug('DBG: remove record: {}={}'.format(rid, get_record(red, rid)))
    red.srem(rids, rid)
    
def incr_error_count(red, rid):
    red.hincrby(ecnt, rid)

def get_error_count(red, rid):
    return int(red.hget(ecnt, rid))

# From REDIS official doc (http://redis.io/commands/save):
#   "The SAVE commands performs a synchronous save of the dataset producing
#   a point in time snapshot of all the data inside the Redis instance, in
#   the form of an RDB file.
#   
#   You almost never want to call SAVE in production environments where it
#   will block all the other clients. Instead usually BGSAVE is
#   used. However in case of issues preventing Redis to create the
#   background saving child (for instance errors in the fork(2) system
#   call), the SAVE command can be a good last resort to perform the dump
#   of the latest dataset.
#   
#   Please refer to the persistence documentation for detailed information."
def force_save(red):
    prev = red.lastsave()
    red.save()
    curr= red.lastsave()
    #!logging.debug('REDIS Saved: {}. (Previously: {}'.format(curr, prev))
    return curr

def push_to_active(red, rid):
    #!logging.debug('push_to_active({})'.format(rid))
    red.lpush(aq, rid)
    red.sadd(aqs, rid)
    #!red.sadd(rids, rid)
    #!red.hmset(rid, rec)

def push_to_inactive(red, rid):
    #!logging.debug('push_to_inactive({})'.format(rid))
    if red.sismember(iqs, rid) == 1:
        logging.debug('Already on INACTIVE queue. Not adding again. {}'
                      .format(rid))
    else:
        red.lpush(iq, rid)
        red.sadd(iqs, rid)
        #!red.sadd(rids, rid)

def queue_summary(red):
    val = red.get(actionP) 
    actionPval = 'on' if val == None else val.decode()
    val = red.get(readP)
    readPval = 'on' if val == None else val.decode()
    return(dict(
        lenActive=red.llen(aq),
        lenInactive=red.llen(iq),
        numRecords=red.scard(rids),
        actionP=actionPval,
        actionPkey=actionP,
        readP=readPval,
        readPkey=readP,
        ))

def log_queue_summary(red):
    dd = queue_summary(red)
    del dd['actionP'], dd['actionPkey'], dd['readP'], dd['readPkey']
    #!logging.debug('Q Summary: {}'.format(dd))

def log_queue_record(red, rid, msg=''):
    logging.debug('Q record: {}{}={}'.format(msg, rid, get_record(red, rid)))


def clear_trans(pl, red=None):
    'REDIS transaction for clearing TADA elements from REDIS'
    ids = red.smembers(rids)
    #!logging.debug('Clearing REDIS DB; ids({})={}'.format(len(ids), ids))
    if len(ids) > 0:
        pl.delete(*ids)
    pl.delete(aq, aqs, iq, iqs, rids, ecnt, actionP, readP, dummy)
    pl.set(actionP, 'on')
    pl.set(readP, 'on')

def queue_full(host, actual_qsize, max_qsize):    
    logging.error(('Queue is full on {}!  Not pushing records.'
                  + ' Current size ({}) > max ({}).'
                  + ' Turning off read from socket.'
                  + ' Disabling push to queue.'
                  + ' To reenable: "dqcli --read on"')
                  .format(host, actual_qsize, max_qsize) )
    r.set(readP, 'off')

##############################################################################

def push_records(host, port, records, max_qsize):
    'records :: list(dict(filename, checksum))'
    r = redis.StrictRedis(host=host, port=port,
                          socket_keepalive=True,
                          retry_on_timeout=True )
    if r.get(readP) == 'off':
        return False
    

    if r.llen(aq) > max_qsize:
        queue_full(host, r.llen(aq), max_qsize)
        return False
    
    for rec in records:
        if len(rec) < 2:
            raise Exception('Attempted to push invalid record={}'.format(rec))
        checksum = rec['checksum']
        if r.sismember(aqs, checksum) == 1:
            logging.warning(': Record for {} is already in queue.'
                            ' Ignoring duplicate.'.format(checksum))
            continue
        # buffer all commands done by pipeline, make command list atomic
        with r.pipeline() as pl:
            while True: # retry if clients collide on watched variables
                try:
                    pl.watch(rids, aq, aqs, checksum)
                    pl.multi()
                    # add to DB
                    #!pl.lpush(aq, checksum)
                    #!pl.sadd(aqs, checksum)
                    push_to_active(pl, checksum)
                    #!pl.sadd(rids, checksum)
                    #!pl.hmset(checksum, rec)
                    #!pl.hset(ecnt, checksum, 0) # error count against file
                    set_record(pl, checksum, rec)
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
    
def push_direct(redis_host, redis_port, fname, checksum):
    'Directly push a record to (possibly remote) REDIS'
    max_queue_size = settings.max_queue_size

    r = redis.StrictRedis(host=redis_host, port=redis_port,
                          socket_keepalive=True,
                          retry_on_timeout=True )

    if r.get(readP) == 'off':
        logging.error('DQ Read from socket is turned off! Not pushing records.')
        return False

    if r.sismember(aqs, checksum) == 1:
        logging.warning('Record for {} is already in queue. Ignoring duplicate.'
                        .format(checksum))
        return False

    if r.llen(aq) > max_queue_size:
        queue_full(redis_host, r.llen(aq), max_queue_size)
        return False
    
    rec = dict(filename=fname, checksum=checksum)

    # buffer all commands done by pipeline, make command list atomic
    with r.pipeline() as pl:
        while True: # retry if clients collide on watched variables
            try:
                pl.watch(rids, aq, aqs, checksum)
                pl.multi()
                # add to DB
                #!pl.sadd(aqs, checksum)
                #!pl.lpush(aq, checksum)
                push_to_active(pl, checksum)
                #!pl.sadd(rids, checksum)
                #!pl.hmset(checksum, rec)
                #!pl.hset(ecnt, checksum, 0) # error count against file
                set_record(pl, checksum, rec)
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

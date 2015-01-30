#! /usr/bin/env python3
'''\
Provide commands (switches) that can be run to modify or display the 
data queue.
'''

import argparse
import sys
import logging
import logging.config
import logging.handlers
import pprint
import json
import fileinput

import redis

from tada import config
from . import dqutils
from . import defaultCfg
from .dbvars import *
from .loggingCfg import *

#! dq_logger = logging.getLogger('dataq.cli')

def clear_db(red):
    'Delete queue related data from DB'
    logging.info(': Resettimg everything related to data queue in redis DB.')
    pl = red.pipeline()
    ids = red.smembers(rids)
    pl.watch(rids,aq,aqs,iq,iqs,*ids)
    pl.multi()
    
    if red.scard(rids) > 0:
        pl.hdel(ecnt,*ids) # clear error counts
        pl.delete(*ids)
    pl.delete(aq,aqs,iq,iqs,rids)
    if pl.get(actionP) == None:
        pl.set(actionP,'on')
    if pl.get(readP) == None:
        pl.set(readP,'on')
    pl.execute()

def info(red):
    pprint.pprint(red.info())

def summary(red):
    'Summarize queue contents.'
    if red.get(actionP) == None:
        red.set(actionP,'on')
    if red.get(readP) == None:
        red.set(readP,'on')

    prms = dict(
        lenActive = red.llen(aq),
        lenInactive = red.llen(iq),
        numRecords = red.scard(rids),
        actionP = red.get(actionP).decode(),
        actionPkey = actionP,
        readP = red.get(readP).decode(),
        readPkey = readP,
        )
    print('''
Active queue length:   %(lenActive)d
Inactive queue length: %(lenInactive)d
Num records tracked:   %(numRecords)d
ACTIONS enabled:       %(actionP)s [%(actionPkey)s]
Socket READ enabled:   %(readP)s [%(readPkey)s]
''' % prms)

def list_queue(red, which):
    'List the content of the queue.'
    if which == 'records':
        print(('Records (%d):'  % (red.scard(rids),)))
        for ridB in sorted(red.smembers(rids)):
            rid = ridB.decode()
            rec = dqutils.decode_dict(red.hgetall(rid))
            kvlist = sorted(list(rec.items()), key=lambda x: x[0])
            print(rid,':',', '.join(['%s=%s'%(k,v) for (k,v) in kvlist]))
        return 

    if which == 'active':
        q = aq
    else:
        q = iq
    id_list = red.lrange(q, 0, -1)
    print(('%s QUEUE (%s):'  % (which, len(id_list))))
    for ridB in id_list:
        rid = ridB.decode()
        rec = dqutils.decode_dict(red.hgetall(rid))
        kvlist = sorted(list(rec.items()), key=lambda x: x[0])
        print(rid,':',', '.join(['%s=%s'%(k,v) for (k,v) in kvlist]))

def dump_queue(red, outfile):
    'Dump copy of queue into this file'
    ids = red.lrange(aq, 0, -1)
    activeIds = set(ids)
    for ridB in ids:
        rid = ridB.decode()
        rec = dqutils.decode_dict(red.hgetall(rid))
        print('%s %s %s'%(rec['filename'], rid, rec['size']),
              file=outfile,
              flush=True
          )



def push_queue(red, infiles):
    'Push records (lines) from list of files. (or stdin if infiles is empty).'
    warnings = 0
    loaded = 0

    with fileinput.input(files=infiles) as infile:
        for line in infile:
            prio = 0
            #!(fname, checksum, size) = line.strip().split()
            #!rec = dict(list(zip(['filename', 'size'], [fname, int(size)])))
            (checksum, fname, *others) = line.strip().split()
            count = 0 if len(others) == 0 else int(others[0])
            rec = dict(filename=fname, checksum=checksum, error_count=count)

            pl = red.pipeline()
            pl.watch(rids, aq, aqs, checksum)
            pl.multi()
            logging.debug(': Read line with id=%s', checksum)
            if red.sismember(aqs, checksum) == 1:
                logging.warning(': Record for %s is already in queue.'
                                  +' Ignoring duplicate.', checksum)
                warnings += 1
            else:
                # add to DB
                pl.sadd(aqs, checksum)
                pl.lpush(aq, checksum)
                pl.sadd(rids, checksum)
                pl.hmset(checksum, rec)
                pl.save()
                loaded += 1
                pl.execute()
        print('PUSH: Issued %d warnings. %d loaded'%(warnings, loaded))

def push_string(red, line):
    'Push record (string) containing: "checksum filename"'
    warnings = 0
    loaded = 0

    prio = 0
    (checksum, fname, *others) = line.strip().split()
    count = 0 if len(others) == 0 else int(others[0])
    rec = dict(filename=fname, checksum=checksum, error_count=count)

    pl = red.pipeline()
    pl.watch(rids, aq, aqs, checksum)
    pl.multi()
    logging.debug(': Read line with id=%s', checksum)
    if red.sismember(aqs, checksum) == 1:
        logging.warning(': Record for %s is already in queue.'
                          +' Ignoring duplicate.', checksum)
        warnings += 1
    else:
        # add to DB
        pl.sadd(aqs, checksum)
        pl.lpush(aq, checksum)
        pl.sadd(rids, checksum)
        pl.hmset(checksum, rec)
        pl.save()
        loaded += 1
        pl.execute()
    print('PUSH: Issued %d warnings. %d loaded'%(warnings, loaded))

def get_selected(ids, first, last):
    selected = ids[ids.index(first):ids.index(last)+1]
    if len(selected) == 0:
        selected = ids[ids.index(last):ids.index(first)+1]
    return selected

def advance_range(red, first, last):
    '''Move range of records incluing FIRST and LAST id from where
    ever they are on the queue to the tail (they will become next to
    pop)'''
    pl = red.pipeline()
    pl.watch(aq)
    pl.multi()

    ids = [b.decode() for b in red.lrange(aq, 0, -1)]
    try:
        selected = get_selected(ids, first, last)
        logging.debug('Selected records = %s', selected)
    except:
        logging.error('IGNORED. Could not select [{}:{}] from {}.'
                        .format(first, last, ids))
        return
    

    # move selected IDs to the tail
    for rid in selected:
        pl.lrem(aq, 0, rid)
        # rpush doesn't seem to work with multi values so I can't do
        # all SELECTED at once.
        pl.rpush(aq, rid)
        pl.save()
        pl.execute()
        print('Advanced %d records to next-in-line' % (len(selected),))

def deactivate_range(red, first, last):
    '''Move range of records including FIRST and LAST id from where
    they are on the active queue to the head of INACTIVE queue.'''
    pl = red.pipeline()
    pl.watch(aq, aqs, iq)
    pl.multi()

    ids = [b.decode() for b in red.lrange(aq, 0, -1)]
    try:
        selected = get_selected(ids, first, last)
        logging.debug('Selected records = %s', selected)
    except:
        logging.error('Could not select [{}:{}] from {}.'
                        .format(first, last, ids))
        raise

    for rid in selected:
        if red.sismember(iqs, rid) == 1:
            logging.warning(': Record for %s is already in inactive queue.'
                              +' Ignoring duplicate.', rid)
            warnings += 1
        else:
            pl.lrem(aq, 0, rid)
            pl.srem(aqs, rid)
            pl.lpush(iq, rid)
            pl.sadd(iqs, rid)

        pl.save()
        pl.execute()
        print('Deactivated %d records' % (len(selected),))


def activate_ids(red, selected):
    warnings = 0
    moved = 0
    pl = red.pipeline()
    pl.watch(aq,aqs,iq)
    pl.multi()

    for rid in selected:
        if red.sismember(aqs, rid) == 1:
            logging.warning(': Record for %s is already in active queue.'
                              +' Ignoring duplicate.', rid)
            warnings += 1
        else:
            moved += 1
            pl.lrem(iq, 0, rid)
            pl.srem(iqs, rid)
            pl.sadd(aqs, rid)
            pl.rpush(aq, rid)
        pl.save()
        pl.execute()
    return warnings, moved

def activate_all(red):
    '''Move ALL records from INACTIVE queue to the tail of ACTIVE queue.'''
    ids = [b.decode() for b in red.lrange(iq, 0, -1)]
    warnings, moved = activate_ids(red, ids)
    print('Activated {} records ({} were already active)'
          .format(moved, warnings))
    return moved

def activate_range(red, first, last):
    '''Move range of records including FIRST and LAST id from where
    they are on the INACTIVE queue to the tail of ACTIVE queue.'''

    ids = [b.decode() for b in red.lrange(iq, 0, -1)]
    logging.debug('ids = %s', ids)
    try:
        selected = get_selected(ids, first, last)
        logging.debug('Selected records (first,last) = (%s,%s) %s',
                        first, last, selected)
    except:
        logging.error('IGNORED. Could not select [{}:{}] from {}.'
                        .format(first, last, ids))
        return


    warnings, moved = activate_ids(red, selected)
    print('Activated %d records' % moved)
    return moved


##############################################################################


def main():
    'Parse command line (a mini-interpreter) and do the work.'
    possible_qnames = ['transfer', 'submit', 'mitigate']
    parser = argparse.ArgumentParser(
        description='Modify or display the data queue',
        epilog='EXAMPLE: %(prog)s --summary'
    )
    #!parser.add_argument('--host',
    #!                    help='Host to bind to',
    #!                    default='localhost')
    #!parser.add_argument('--port',
    #!                    help='Port to bind to',
    #!                    type=int, default=9988)
    parser.add_argument('--cfg',
                        help='Configuration file',
                        type=argparse.FileType('r'))
    parser.add_argument('--queue', '-q',
                        choices=possible_qnames,
                        help='Name of queue to pop from. Must be in cfg file.')

    parser.add_argument('--summary', '-s',
                        help='Show summary of queue contents.',
                        action='store_true')
    parser.add_argument('--info', '-i', help='Show info about Redis server.',
                        action='store_true')
    parser.add_argument('--list', '-l',
                        help='List queue',
                        choices=['active', 'inactive', 'records'])
    parser.add_argument('--action', '-a',
                        help='Turn on/off running actions on queue records.',
                        default=None,
                        choices=['on', 'off'])
    parser.add_argument('--read', '-r',
                        help='Turn on/off reading socket and pushing to queue.',
                        default=None,
                        choices=['on', 'off'])
    parser.add_argument('--clear', help='Delete queue related data from DB',
                        action='store_true')

    parser.add_argument('--dump',
                        help='Dump copy of queue into this file',
                        type=argparse.FileType('w'))
    parser.add_argument('--push',
                        help='File of data records to load into queue.'
                        +' Multiple allowed.  Use "-" for stdin',
                        action='append')
    parser.add_argument('--pushstr',
                        help='A single string to load into queue.'
                        +' Space delimited string must contain at least'
                        +' "checksum filename".')

    parser.add_argument('--advance',
                        help='Move records to end of queue.',
                        nargs=2)

    parser.add_argument('--deactivate',
                        help='Move selected records to INACTIVE',
                        nargs=2)
    parser.add_argument('--activate',
                        help='Move selected records to ACTIVE',
                        nargs=2)
    parser.add_argument('--redo',
                        help='Move ALL records to ACTIVE',
                        action='store_true'
                        )

    parser.add_argument('--loglevel',
                        help='Kind of diagnostic output',
                        choices=['CRTICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='WARNING',
    )
    args = parser.parse_args()


    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        parser.error('Invalid log level: %s' % args.loglevel) 
        logging.config.dictConfig(LOG_SETTINGS)



    #!logging.basicConfig(level = numeric_level,
    #!                    format='%(levelname)s %(message)s',
    #!                    datefmt='%m-%d %H:%M',
    #!                    )
    #!logfilename='/var/log/dataq.log'
    #!handler = logging.handlers.RotatingFileHandler(logfilename,
    #!                                               maxBytes=1e4,
    #!                                               backupCount=9,
    #!                                               )
    #!logging.addHandler(handler)
    logging.debug('Debug output is enabled!!')
    #logging.info('EXECUTING: %s',' '.join(sys.argv))


    ############################################################################
    #! cfg = defaultCfg.cfg if args.cfg is None else json.load(args.cfg)
    #! qcfg = config.get_config(possible_qnames)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    red = redis.StrictRedis()
    if args.clear:
        clear_db(red)

    if args.action is not None:
        red.set(actionP, args.action)
        red.lpush(dummy, 'ignore')
        if args.read is not None:
            red.set(readP, args.read)


    if args.list:
        list_queue(red, args.list)

    if args.dump:
        dump_queue(red, args.dump)

    if args.push:
        push_queue(red, args.push)
    if args.pushstr:
        push_string(red, args.pushstr)

    if args.advance:
        advance_range(red, args.advance[0], args.advance[1])

    if args.deactivate:
        deactivate_range(red, args.deactivate[0], args.deactivate[1])

    if args.activate:
        activate_range(red, args.activate[0], args.activate[1])

    if args.redo:
        activate_all(red)

    if args.info:
        info(red)
        if args.summary:
            summary(red)

    if args.summary:
        summary(red)

    red.save()

if __name__ == '__main__':
    main()

#! /usr/bin/env python
'''\
Provide commands (switches) that can be run to modify or display the 
data queue.
'''

import os, sys, string, argparse, logging
import pprint
import redis
from dbvars import *
import utils

def clear_db(r):
    logging.info(': Resettimg everything related to data queue in redis DB.')
    pl = r.pipeline()
    ids = r.smembers(rids)
    pl.watch(rids,aq,aqs,iq,*ids)
    pl.multi()
    
    if r.scard(rids) > 0:
        pl.delete(*ids)
    pl.delete(aq,aqs,iq,rids)
    if pl.get(actionP) == None:
        pl.set(actionP,'on')
    if pl.get(readP) == None:
        pl.set(readP,'on')
    pl.execute()

def info(r):
    pprint.pprint(r.info())

def summary(r):
    if r.get(actionP) == None:
        r.set(actionP,'on')
    if r.get(readP) == None:
        r.set(readP,'on')

    prms = dict(
        lenActive = r.llen(aq),
        lenInactive = r.llen(iq),
        numRecords = r.scard(rids),
        actionP = r.get(actionP).decode(),
        actionPkey = actionP,
        readP = r.get(readP).decode(),
        readPkey = readP,
        )
    print('''
Active queue length:   %(lenActive)d
Inactive queue length: %(lenInactive)d
Num records tracked:   %(numRecords)d
ACTIONS enabled:       %(actionP)s [%(actionPkey)s]
Socket READ enabled:   %(readP)s [%(readPkey)s]
''' % prms)

def list_queue(r,which):
    if which == 'records':
        print(('Records (%d):'  % (r.scard(rids),)))
        for ridB in r.smembers(rids):
            rid = ridB.decode()
            rec = utils.decode_dict(r.hgetall(rid))
            print(rid,':',', '.join(['%s=%s'%(k,v) for (k,v) in list(rec.items())]))
        return 

    if which == 'active':
        q = aq
    else:
        q = iq
    id_list = r.lrange(q,0,-1)
    print(('%s QUEUE (%s):'  % (which, len(id_list))))
    for ridB in id_list:
        rid = ridB.decode()
        rec = utils.decode_dict(r.hgetall(rid))
        print(rid,':',', '.join(['%s=%s'%(k,v) for (k,v) in list(rec.items())]))

    
def dump_queue(r, outfile):
    ids = r.lrange(aq,0,-1)
    activeIds = set(ids)
    for ridB in ids:
        rid = ridB.decode()
        rec = utils.decode_dict(r.hgetall(rid))
        print('%s %s %s'%(rec['filename'],rid,rec['size']), file=outfile)



def load_queue(r, infile):
    warnings = 0
    loaded = 0

    for line in infile:
        prio = 0
        (fname,checksum,size) = line.strip().split()
        rec = dict(list(zip(['filename','size'],[fname,int(size)])))

        pl = r.pipeline()
        pl.watch(rids,aq,aqs,checksum)
        pl.multi()
        logging.debug(': Read line with id=%s',checksum)
        if r.sismember(aqs,checksum) == 1:
            logging.warning(': Record for %s is already in queue.' 
                            +' Ignoring duplicate.', checksum)
            warnings += 1
        else:
            # add to DB
            pl.sadd(aqs,checksum) 
            pl.lpush(aq,checksum)
            pl.sadd(rids,checksum) 
            pl.hmset(checksum,rec)
            pl.save()    
            loaded += 1
        pl.execute()
    print('LOAD: Issued %d warnings. %d loaded'%(warnings,loaded))
    

def get_selected(ids,first,last):
    selected = ids[ids.index(first):ids.index(last)+1]
    if len(selected) == 0:
        selected = ids[ids.index(last):ids.index(first)+1]
    return selected
    
def advance_range(r,first,last):
    '''Move range of records incluing FIRST and LAST id from where
    ever they are on the queue to the tail (they will become next to
    pop)'''
    pl = r.pipeline()
    pl.watch(aq)
    pl.multi()

    ids = [b.decode() for b in r.lrange(aq,0,-1)]
    selected = get_selected(ids,first,last)
    logging.debug('Selected records = %s',selected)

    # move selected IDs to the tail
    for rid in selected:
        pl.lrem(aq,0,rid)
        # rpush doesn't seem to work with multi values so I can't do
        # all SELECTED at once.
        pl.rpush(aq,rid)
    pl.save()    
    pl.execute()    
    print('Advanced %d records to next-in-line' % (len(selected),))

def deactivate_range(r,first,last):
    '''Move range of records incluing FIRST and LAST id from where
    they are on the active queue to the head of INACTIVE queue.'''
    pl = r.pipeline()
    pl.watch(aq,aqs,iq)
    pl.multi()

    ids = [b.decode() for b in r.lrange(aq,0,-1)]
    selected = get_selected(ids,first,last)
    logging.debug('Selected records = %s',selected)
   
    for rid in selected:
        if r.sismember(iqs,rid) == 1:
            logging.warning(': Record for %s is already in inactive queue.' 
                            +' Ignoring duplicate.', rid)
            warnings += 1
        else:
            pl.lrem(aq,0,rid)
            pl.srem(aqs,rid)
            pl.lpush(iq,rid)
            pl.sadd(iqs,rid)
    
    pl.save()    
    pl.execute()    
    print('Deactivated %d records' % (len(selected),))

def activate_range(r,first,last):
    '''Move range of records incluing FIRST and LAST id from where
    they are on the INACTIVE queue to the tail of ACTIVE queue.'''
    warnings = 0
    moved = 0
    pl = r.pipeline()
    pl.watch(aq,aqs,iq)
    pl.multi()

    ids = [b.decode() for b in r.lrange(iq,0,-1)]
    logging.debug('ids = %s',ids)
    selected = get_selected(ids,first,last)

    logging.debug('Selected records (first,last) = (%s,%s) %s',
                  first,last, selected)
   
    for rid in selected:
        if r.sismember(aqs,rid) == 1:
            logging.warning(': Record for %s is already in active queue.' 
                            +' Ignoring duplicate.', rid)
            warnings += 1
        else:
            moved += 1
            pl.lrem(iq,0,rid)
            pl.srem(iqs,rid)
            pl.sadd(aqs,rid)
            pl.rpush(aq,rid)
    pl.save()    
    pl.execute()    
    print('Activated %d records' % moved)


##############################################################################


def main():
    #!print('EXECUTING: %s\n\n' % (string.join(sys.argv)))
    parser = argparse.ArgumentParser(
        description='Modify or display the data queue',
        epilog='EXAMPLE: %(prog)s --summary'
        )
    parser.add_argument('--host',  help='Host to bind to',
                        default='localhost')
    parser.add_argument('--port',  help='Port to bind to',
                        type=int, default=9988)
    parser.add_argument('--summary',  help='Show summary of queue contents.',
                        action='store_true' )
    parser.add_argument('--info',  help='Show info about Redis server.',
                        action='store_true' )
    parser.add_argument('--list',  help='List queue',
                        choices=['active','inactive','records'],
                        )
    parser.add_argument('--action',  
                        help='Turn on/off running actions on queue records.',
                        default=None,
                        choices=['on','off'],
                        )
    parser.add_argument('--read',  
                        help='Turn on/off reading socket and pushing to queue.',
                        default=None,
                        choices=['on','off'],
                        )
    parser.add_argument('--clear',  help='Delete queue related data from DB',
                        action='store_true' )

    parser.add_argument('--dump', help='Dump copy of queue into this file',
                        type=argparse.FileType('w') )
    parser.add_argument('--load', 
                        help='File of data records to load into queue',
                        type=argparse.FileType('r') )

    parser.add_argument('--advance',  help='Move records to end of queue.',
                        nargs=2 )

    parser.add_argument('--deactivate',  help='Move records to INACTIVE',
                        nargs=2 )
    parser.add_argument('--activate',  help='Move records to ACTIVE',
                        nargs=2 )

    parser.add_argument('--loglevel',      help='Kind of diagnostic output',
                        choices = ['CRTICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='WARNING',
                        )
    args = parser.parse_args()


    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        parser.error('Invalid log level: %s' % args.loglevel) 
    logging.basicConfig(level = log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M'
                        )
    logging.debug('Debug output is enabled!!')
    ############################################################################

    r = redis.StrictRedis()
    if args.clear:
        clear_db(r)

    if args.action is not None:
        r.set(actionP,args.action)
    if args.read is not None:
        r.set(readP,args.read)


    if args.list:
        list_queue(r,args.list)
        
    if args.dump:
        dump_queue(r, args.dump)

    if args.load:
        load_queue(r, args.load)
    
    if args.advance:
        advance_range(r, args.advance[0], args.advance[1])

    if args.deactivate:
        deactivate_range(r, args.deactivate[0], args.deactivate[1])

    if args.activate:
        activate_range(r, args.activate[0], args.activate[1])
        
    if args.info:
        info(r)
    if args.summary:
        summary(r)

    r.save()

if __name__ == '__main__':
    main()

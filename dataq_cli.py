#! /usr/bin/env python
'''\
Modify the data queue (managed by: dataq_svc.py)
'''

import os, sys, string, argparse, logging
from pprint import pprint 
import redis

aq = 'activeq' # Active Queue. List of IDs. Pop and apply actions from this.
iq = 'inactiveq' # List of IDs. Stash records that will not be popped here
ecnt = 'errorcnt' # errorcnt[id] = cnt; number of Action errors against ID
actionP = 'actionFlag' # on|off
readP = 'readFlag' # on|off


def summary(r):
    prms = dict(
        lenActive = r.llen(aq),
        lenInactive = r.llen(iq),
        actionP = r.get(actionP),
        actionPkey = actionP,
        readP = r.get(readP),
        readPkey = readP,
        )
    print '''
Active queue length:   %(lenActive)d
Inactive queue length: %(lenInactive)d
ACTIONS enabled:       %(actionP)s [%(actionPkey)s]
Socket READ enabled:   %(readP)s [%(readPkey)s]
''' % prms

def list_queue(r,which):
    if which == 'active':
        q = aq
    else:
        q = iq

    id_list = r.lrange(q,0,-1)
    print('ACTIVE QUEUE (%s):'  % (len(id_list),))
    for rid in id_list:
        rec = r.hgetall(rid)
        print '%s: %s'%(rid,rec)
        #print('%s: %s'%(rid,rec['filename']))

    
def dump_queue(r, outfile):
    ids = r.lrange(aq,0,-1)
    activeIds = set(ids)
    for rid in ids:
        rec = r.hgetall(rid)
        print >> outfile, '%s %s %s'%(rec['filename'],rid,rec['size'])



def load_queue(r, infile):
    lut = dict() # lut[rid] => dict(filename,checksum,size,prio)
    warnings = 0

    #!list_activeq(r, msg='Before Reading') # DBG

    # stuff local structures from DB
    activeIds = set(r.lrange(aq,0,-1))
    for rid in activeIds:
        lut[rid] = r.hgetall(rid)

    for line in infile:
        prio = 0
        (fname,checksum,size) = line.strip().split()
        if checksum in activeIds:
            logging.warning(': Record for %s is already in queue.' 
                            +' Ignoring duplicate.', checksum)
            warnings += 1
            continue
        
        activeIds.add(checksum)
        rec = dict(list(zip(['filename','size'],[fname,int(size)])))
        lut[checksum] = rec
        
        # add to DB
        r.lpush(aq,checksum)
        r.hmset(checksum,rec)

    # DBG
    #! list_activeq(r, msg='After Reading')
    print('Issued %d warnings'%warnings)
    
    
    
def advance_range(r,first,last):
    ids = r.lrange(aq,0,-1)
    print 'first=%s, last=%s'%(first,last)

    selected = ids[ids.index(first):ids.index(last)+1]
    print 'Selected records = ',selected
   
    for rid in selected:
        r.lrem(aq,0,rid)
        # rpush doesn't seem to work with multi values
        for sid in selected:
            r.rpush(aq,sid)
    
    print 'Advanced %d records to next in line'%(len(selected),)

##############################################################################

def main_tt():
    cmd = 'MyProgram.py foo1 foo2'
    sys.argv = cmd.split()
    res = main()
    return res


def main():
    print('EXECUTING: %s\n\n' % (string.join(sys.argv)))
    parser = argparse.ArgumentParser(
        version='1.0.1',
        description='My shiny new python program',
        epilog='EXAMPLE: %(prog)s a b"'
        )
    parser.add_argument('--host',  help='Host to bind to',
                        default='localhost')
    parser.add_argument('--port',  help='Port to bind to',
                        type=int, default=9988)
    parser.add_argument('--summary',  help='Show summary of queue contents.',
                        action='store_true' )
    parser.add_argument('--list',  help='List queue',
                        choices=['active','inactive'],
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
    parser.add_argument('--clear',  help='Delete content of queue',
                        action='store_true' )

    parser.add_argument('--dump', help='Dump copy of queue into this file',
                        type=argparse.FileType('w') )
    parser.add_argument('--load', 
                        help='File of data records to load into queue',
                        type=argparse.FileType('r') )
    parser.add_argument('--advance',  help='Move records to end of queue.',
                        nargs=2 )

    parser.add_argument('--deactivate',  help='Move records to INACTIVE',
                        action='store_true' )
    parser.add_argument('--activate',  help='Move records to ACTIVE',
                        action='store_true' )

    parser.add_argument('--loglevel',      help='Kind of diagnostic output',
                        choices = ['CRTICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='WARNING',
                        )
    args = parser.parse_args()
    #!args.outfile.close()
    #!args.outfile = args.outfile.name

    #!print 'My args=',args
    #!print 'infile=',args.infile

    #!validateQuality(parser, args.quality)
    #!if not os.path.isfile(args.infile):
    #!    parser.error('Cannot find input NTF "%s""'%(args.infile,))


    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        parser.error('Invalid log level: %s' % args.loglevel) 
    logging.basicConfig(level = log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M'
                        )
    logging.debug('Debug output is enabled by nitfConvert!!!')

    r = redis.StrictRedis()
    if args.action is not None:
        r.set(actionP,args.action)
    if args.read is not None:
        r.set(readP,args.read)

    if args.clear:
        r.flushall() # overkill !!!
        r.set(actionP,'on')
        r.set(readP,'on')

    if args.list:
        list_queue(r,args.list)
        
    if args.dump:
        dump_queue(r, args.dump)

    if args.load:
        load_queue(r, args.load)
    
    if args.advance:
        advance_range(r, args.advance[0], args.advance[1])
        
    if args.summary:
        summary(r)

    r.bgsave()

if __name__ == '__main__':
    main()

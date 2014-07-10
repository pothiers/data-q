#! /usr/bin/env python
'''\
Process records of incoming data files, apply various actions on pop,
be resilient.

The checksum provided for each data record is used as an ID.  We treat
it as "mostly unique"; with special handling for when its not unique.

To tell if two records are identical, we check equality of ALL three of: 
   checksum
   full file name
   file size
'''

import os, sys, string, argparse, logging
from pprint import pprint 
import random
import redis

aq = 'activeq' # Active Queue. List of IDs. Pop and apply actions from this.
iq = 'inactiveq' # List of IDs. Stash records that will not be popped here

def echo(rec, probFail = 0.05):
    print 'Processing file: %s' % rec['filename']
    # !!! randomize success to simulate errors on cmds
    return random.random() > probFail


cfg = dict(
    action = echo,
    )


def listAQ(r, msg='DBG'):
    print '~'*60
    print '%s; ACTIVE QUEUE (%s):'  % (msg,r.llen(aq))
    for rid in r.lrange(aq,0,-1):
        rec = r.hgetall(rid)
        #! print '%s: %s'%(rid,rec)
        print '%s: %s'%(rid,rec['filename'])

    

def loadQueue(r, infile, clear):
    lut = dict() # lut[rid] => dict(filename,checksum,size,prio)
    warnings = 0
    
    if clear:
        r.flushall() # overkill !!!

    listAQ(r, msg='Before Reading') # DBG

    # stuff local structures from DB
    ids = r.lrange(aq,0,-1)
    activeIds = set(ids)
    for rid in ids:
        lut[rid] = r.hgetall(rid)

    for line in infile:
        prio = 0
        (fname,checksum,size) = line.strip().split()
        if checksum in activeIds:
            # checksum clashed
            rec0 = lut[checksum]
            if ((rec0['filename'] == fname) and (rec0['size'] == size)):
                # IDENTICAL record
                logging.warning(': Record for %s is already in queue.' 
                                +' Ignoring new duplicate.', checksum)
                warnings += 1
                continue
            else:
                # DIFFERENT record
                logging.warning(
                        ': Checksum (%s) is already in queue.'
                        +'You will not be able to issue a REMOVE command on it.',
                        checksum)
                warnings += 1
                
        activeIds.add(checksum)
        rec = dict(zip(['filename','size'],[fname,int(size)]))
        lut[checksum] = rec
        
        # add to DB
        r.lpush(aq,checksum)
        r.hmset(checksum,rec)

    # DBG
    listAQ(r, msg='After Reading')
    print 'Issued %d warnings'%warnings
    
        
def processQueue(r): 
    print 'Process Queue'
    while r.llen(aq) > 0:
        rid = r.rpop(aq)
        rec = r.hgetall(rid)
        success = cfg['action'](rec)
        if not success:
            logging.error(': Could not run action (%s) on record (%s)',
                          cfg['action'], rec)
            r.rpush(aq,rid)

    
        
        


##############################################################################

def main_tt():
    cmd = 'MyProgram.py foo1 foo2'
    sys.argv = cmd.split()
    res = main()
    return res

def main():
    print 'EXECUTING: %s\n\n' % (string.join(sys.argv))
    parser = argparse.ArgumentParser(
        version='1.0.1',
        description='My shiny new python program',
        epilog='EXAMPLE: %(prog)s a b"'
        )
    parser.add_argument('infile',  help='Input file',
                        type=argparse.FileType('r') )
    #!parser.add_argument('outfile', help='Output output',
    #!                    type=argparse.FileType('w') )
    parser.add_argument('--clear',  help='Delete content of queue first',
                        action='store_true' )
    parser.add_argument('--loadOnly',  help='Do not process queue',
                        action='store_true' )

    parser.add_argument('-q', '--quality', help='Processing quality',
                        choices=['low','medium','high'], default='high')
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
    loadQueue(r, args.infile, args.clear)
    if not args.loadOnly:
        processQueue(r)

if __name__ == '__main__':
    main()

#! /usr/bin/env python
'''\ 
Pop records from queue and apply action. 

'''

import os, sys, string, argparse, logging
from pprint import pprint 
import random
import redis
import time

aq = 'activeq' # Active Queue. List of IDs. Pop and apply actions from this.
iq = 'inactiveq' # List of IDs. Stash records that will not be popped here
ecnt = 'errorcnt' # errorcnt[id] = cnt; number of Action errors against ID
actionP = 'actionFlag' # on|off
readP = 'readFlag' # on|off

def echo(rec, probFail = 0.10):
    print('Processing record: %s' % rec)
    # !!! randomize success to simulate errors on cmds
    return random.random() > probFail


# !!! Temp config.  Will move external later. (ConfigParser)
cfg = dict(
    actionName = 'echo',
    action = echo,
    )


def process_queue_forever(r, poll_interval=0.5, maxErrPer=3): 
    activeIds = set(r.lrange(aq,0,-1))
    lut = dict() # lut[rid] => dict(filename,checksum,size)
    for rid in activeIds:
        lut[rid] = r.hgetall(rid)

    errorCnt = 0
    print('Process Queue')
    while True:
        #! if r.llen(aq) == 0:
        #!     time.sleep(poll_interval)  
        #!     continue
        if r.get(actionP) == 'off':
            continue

        print 'BLOCKING rpop'
        rid = r.brpop(aq) # BLOCKING pop
        print '...got rpop'
        rec = r.hgetall(rid)
        success = cfg['action'](rec)
        if success:
            print('Action ran successfully against:',rec)            
        else:
            errorCnt += 1
            cnt = r.hincrby(ecnt,rid)
            if cnt > maxErrPer:
                r.lpush(iq,rid)  # kept failing: move to Inactive queue
                logging.warning(
                    ': Failed to run action "%s" on record (%s) %d times.'
                    +' Moving it to the Inactive queue',
                    cfg['actionName'], rec,cnt)

                continue

            logging.error(': Failed to run action "%s" on record (%s) %d times',
                          cfg['actionName'], rec,cnt)
            r.lpush(aq,rid) # failed: got to the end of the line


        


##############################################################################


def main():
    print('EXECUTING: %s\n\n' % (string.join(sys.argv)))
    parser = argparse.ArgumentParser(
        version='1.0.2',
        description='Data Queue service',
        epilog='EXAMPLE: %(prog)s [--host localhost] [--port 9988]"'
        )

    parser.add_argument('--host',  help='Host to bind to',
                        default='localhost')
    parser.add_argument('--port',  help='Port to bind to',
                        type=int, default=9988)


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
    logging.debug('Debug output is enabled by dataq_svc!!!')


    # stuff local structures from DB
    r = redis.StrictRedis()

    process_queue_forever(r)

if __name__ == '__main__':
    main()

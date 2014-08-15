#! /usr/bin/env python3
'''\ 
Pop records from queue and apply action. 
'''

# Probably could use asyncio to good use here.  Didn't know about it
# when I started and maybe our case is easy enuf that it doesn't
# matter. But we do a loop (while True) that smacks of an event loop!!!


import argparse
import logging
import json
import time
import sys

import redis
#!import daemon

import utils
import defaultCfg
from dbvars import *
from actions import *

def process_queue_forever(r, cfg,delay=1.0): 
    action_name = cfg['action_name']
    action = action_lut[action_name]

    errorCnt = 0
    logging.debug('Process Queue')
    while True:
        if r.get(actionP) == b'off':
            time.sleep(delay)
            continue
        
        # ALERT: being "clever" here!
        #
        # If actionP was turned on, and the queue isn't being filled,
        # we will eventually pop everything from the queue and block.
        # But then if actionP is turned OFF, we don't know yet because
        # we are still blocking.  So next queue item will sneak by.
        # Probably unimportant on long running system, but plays havoc
        # with testing. To resolve, on setting actionP to off, we push
        # to dummy to clear block.
        (keynameB,ridB) = r.brpop([dummy,aq]) # BLOCKING pop (over list of keys)
        if keynameB.decode() == dummy:
            continue
        rid = ridB.decode()

        pl = r.pipeline()
        pl.watch(aq,aqs,rids,ecnt,iq,rid)
        pl.multi()

        pl.srem(aqs,rid) 
        rec = utils.decode_dict(r.hgetall(rid))
        
        success = action(rec)
        if success:
            logging.debug('Action ran successfully against (%s): %s',
                          rid,rec)            
            pl.srem(rids,rid) 
        else:
            errorCnt += 1
            cnt = pl.hincrby(ecnt,rid)
            print('Error count for "%s"=%d'%(rid,cnt))
            if cnt > cfg['maximum_errors_per_record']:
                pl.lpush(iq,rid)  # action kept failing: move to Inactive queue
                logging.warning(
                    ': Failed to run action "%s" on record (%s) %d times.'
                    +' Moving it to the Inactive queue',
                    action_name, rec,cnt)
            else:
                logging.error(
                    ': Failed to run action "%s" on record (%s) %d times',
                    action_name, rec, cnt)
                pl.lpush(aq,rid) # failed: got to the end of the line
        pl.save()    
        pl.execute()

##############################################################################


def main():
    parser = argparse.ArgumentParser(
        description='Data Queue service',
        epilog='EXAMPLE: %(prog)s --loglevel DEBUG &'
        )

    parser.add_argument('--host',  help='Host to bind to',
                        default='localhost')
    parser.add_argument('--port',  help='Port to bind to',
                        type=int, default=9988)
    parser.add_argument('--cfg', 
                        help='Configuration file',
                        type=argparse.FileType('r') )


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
    ###########################################################################

    utils.save_pid(sys.argv[0])

    cfg = defaultCfg.cfg if args.cfg is None else json.load(args.cfg)
    process_queue_forever(redis.StrictRedis(), cfg)

if __name__ == '__main__':
    main()


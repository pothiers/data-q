#! /usr/bin/env python
'''\ 
Pop records from queue and apply action. 
'''

import os, sys, string, argparse, logging
import random
import redis
from dbvars import *
from actions import *


# !!! Temp config.  Will move external later. (ConfigParser)
cfg = dict(
    action_name = 'echo10',
    )
cfg['action'] = action_lut[cfg['action_name']]



def process_queue_forever(r, poll_interval=0.5, maxErrPer=3): 
    activeIds = set(r.lrange(aq,0,-1))
    rids =  r.smembers(rids)

    errorCnt = 0
    logging.debug('Process Queue')
    while True:
        if r.get(actionP) == 'off':
            continue

        rid = r.brpop(aq) # BLOCKING pop
        rec = r.hgetall(rid)
        success = cfg['action'](rec)
        if success:
            logging.debug('Action ran successfully against:',rec)            
        else:
            errorCnt += 1
            cnt = r.hincrby(ecnt,rid)
            if cnt > maxErrPer:
                r.lpush(iq,rid)  # kept failing: move to Inactive queue
                logging.warning(
                    ': Failed to run action "%s" on record (%s) %d times.'
                    +' Moving it to the Inactive queue',
                    cfg['action_name'], rec,cnt)

                continue

            logging.error(': Failed to run action "%s" on record (%s) %d times',
                          cfg['action_name'], rec,cnt)
            r.lpush(aq,rid) # failed: got to the end of the line
        r.save()    


##############################################################################


def main():
    #!print('EXECUTING: %s\n\n' % (string.join(sys.argv)))
    parser = argparse.ArgumentParser(
        version='1.0.2',
        description='Data Queue service',
        epilog='EXAMPLE: %(prog)s --host localhost --port 9988'
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
    logging.debug('Debug output is enabled!!')
    ###########################################################################

    process_queue_forever(redis.StrictRedis())

if __name__ == '__main__':
    main()

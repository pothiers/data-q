#! /usr/bin/env python3
""" Pop records from queue and apply action. If action throws error,
put record back on queue.
"""

# Probably could use asyncio to good use here.  Didn't know about it
# when I started and maybe our case is easy enuf that it doesn't
# matter. But we do a loop (while True) that smacks of an event loop!!!


import argparse
import logging
import json
import time
import sys

import redis

from . import config
from . import dqutils
from . import default_config
from .dbvars import *
from .actions import *


def process_queue_forever(qname, qcfg, delay=1.0):
    'Block waiting for items on queue, then process, repeat.'
    red = redis.StrictRedis()
    action_name = qcfg[qname]['action_name']
    action = action_lut[action_name]

    logging.debug('Read Queue "{}"'.format(qname))
    while True:
        #! logging.debug('Read Queue: loop')

        if red.get(actionP) == b'off':
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
        (keynameB, ridB) = red.brpop([dummy, aq]) # BLOCKING pop (over key list)
        if keynameB.decode() == dummy:
            continue
        rid = ridB.decode()
        #!logging.debug('Read Queue: got something')

        pl = red.pipeline()
        pl.watch(aq, aqs, rids, ecnt, iq, rid)
        pl.multi()

        pl.srem(aqs, rid)
        rec = dqutils.decode_dict(red.hgetall(rid))


        try:
            logging.debug('Run action: "%s"(%s)"'%(action_name, rec))
            result = action(rec,qcfg=qcfg)
            logging.debug('Action "%s" ran successfully against (%s): %s => %s',
                          action_name, rid, rec, result)
            pl.srem(rids, rid) # only if action did not raise exception
        except:
            logging.debug('Action "%s" got error'%(action_name,))
            pl.hincrby(ecnt, rid)
            cnt = red.hget(ecnt, rid)
            print('Error count={} for {}'.format(cnt, rid))
            if cnt > qcfg[qname]['maximum_errors_per_record']:
                pl.lpush(iq, rid)  # action kept failing: move to Inactive queue
                logging.warning(
                    ': Failed to run action "%s" on record (%s) %d times.'
                    +' Moving it to the Inactive queue',
                    action_name, rec, cnt)
            else:
                logging.error(
                    ': Failed to run action "%s" on record (%s) %d times',
                    action_name, rec, cnt)
                pl.lpush(aq, rid) # failed: got to the end of the line
        pl.save()
        pl.execute()

##############################################################################


def main():
    'Parse args, then start reading queue forever.'
    possible_qnames = ['transfer', 'submit', 'mitigate']
    parser = argparse.ArgumentParser(
        description='Data Queue service',
        epilog='EXAMPLE: %(prog)s --loglevel DEBUG &'
        )

    #!parser.add_argument('--host',
    #!                    help='Host to bind to',
    #!                    default='localhost')
    #!parser.add_argument('--port',
    #!                    help='Port to bind to',
    #!                    type=int, default=9988)
    parser.add_argument('--cfg',
                        help='Configuration file (json format)',
                        type=argparse.FileType('r'))
    parser.add_argument('--queue', '-q',
                        choices=possible_qnames,
                        help='Name of queue to pop from. Must be in cfg file.')

    parser.add_argument('--loglevel',
                        help='Kind of diagnostic output',
                        choices=['CRTICAL', 'ERROR', 'WARNING',
                                 'INFO', 'DEBUG'],
                        default='WARNING')
    args = parser.parse_args()

    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        parser.error('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M')
    logging.debug('Debug output is enabled!!')
    ###########################################################################

    dqutils.save_pid(sys.argv[0])

    #!cfg = default_config.DQ_CONFIG if args.cfg is None else json.load(args.cfg)
    #!qcfg = dqutils.get_config_lut(cfg)[args.queue]
    qcfg = config.get_config(possible_qnames)
    # red = redis.StrictRedis(host=args.host, port=args.port)
    #! process_queue_forever(red, config)
    process_queue_forever(args.queue, qcfg)

if __name__ == '__main__':
    main()

    

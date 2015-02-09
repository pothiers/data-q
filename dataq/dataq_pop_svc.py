#! /usr/bin/env python3
""" Pop records from queue and apply action. If action throws error,
put record back on queue.
"""

# Probably could use asyncio to good use here.  Didn't know about it
# when I started and maybe our case is easy enuf that it doesn't
# matter. But we do a loop (while True) that smacks of an event loop!!!


import argparse
import logging
import logging.config
import json
import time
import sys
import traceback

import yaml
import redis

from tada import config
from . import dqutils
from . import default_config
from .dbvars import *
from .actions import *

# GROSS: highly nested code!!!
def process_queue_forever(qname, qcfg, dirs, delay=1.0):
    'Block waiting for items on queue, then process, repeat.'
    red = redis.StrictRedis()
    action_name = qcfg[qname]['action_name']
    action = action_lut[action_name]
    maxerrors = qcfg[qname]['maximum_errors_per_record']

    logging.debug('Read Queue "{}"'.format(qname))
    while True: # pop from queue forever
        logging.debug('Read Queue: loop')

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
        logging.debug('Read Queue: got "{}"'.format(rid))
        did_action = False
        success = False

        # buffer all commands done by pipeline, make command list atomic
        with red.pipeline() as pl:
            while True: # retry if clients collide on watched variables
                try:
                    #!pl.watch(aq, aqs, rids, ecnt, iq, rid)
                    pl.watch(aq, aqs, iq)
                    rec = dqutils.decode_dict(pl.hgetall(rid))

                    # switch to normal pipeline mode where commands get buffered
                    pl.multi()

                    pl.srem(aqs, rid)
                    try:
                        #Wait "seconds_between_retry" if ecnt > 0 !!!
                        if not did_action:
                            logging.debug('RUN action: "{}"; {}"'
                                          .format(action_name, rec))
                            result = action(rec, qname, qcfg=qcfg, dirs=dirs)
                            logging.debug('DONE action: "{}" => {}'
                                          .format(action_name, result))
                            did_action = True
                    except Exception as ex:
                        # action failed
                        success = False
                        logging.debug('Action "{}" failed: {}'
                                      .format(action_name, ex))
                        dqutils.traceback_if_debug()
                        pl.hincrby(ecnt, rid)

                        # pl.hget() returns StrictPipeline; NOT value of key!
                        # use of RED here causes us to not get incremented value
                        ercnt = int(red.hget(ecnt,rid) or 0) + 1 
                        #!erall = red.hgetall(ecnt)
                        logging.debug('Got error on action. ercnt={}'
                                      .format(ercnt))
                        #!cnt = 0 if ercnt == None else ercnt
                        cnt = ercnt
                        logging.debug('Error(#{}) running action "{}"'
                                      .format(cnt, action_name))
                        if cnt > maxerrors:
                            msg = ('Failed to run action "{}" {} times. '
                                   +' Max allowed is {} so moving it to the'
                                   +' INACTIVE queue.'
                                   +' Record={}. Exception={}')
                            logging.error(msg.format(action_name,
                                                     cnt, maxerrors, rec, ex))
                            # action kept failing: move to Inactive queue
                            pl.lpush(iq, rid)  
                            # Person should monitor INACTIVE queue!!!
                        else:
                            msg = ('Failed to run action "{}" {} times. '
                                   +' Max allowed is {} so will try again'
                                   +' later.'
                                   +' Record={}. Exception={}')
                            logging.error(msg.format(action_name,
                                                     cnt, maxerrors, rec, ex))
                            # failed: go to the end of the line
                            pl.lpush(aq, rid) 
                    else:
                        success = True
                        #!pl.srem(rids, rid)
                    pl.save()
                    pl.execute() # execute the pipeline
                    break
                except redis.WatchError as ex:
                    logging.debug('Got redis.WatchError: {}'.format(ex))
                    # another client must have changed  watched vars between
                    # the time we started WATCHing them and the pipeline's
                    # execution. Our best bet is to just retry.
                    continue # while True
        # END with pipeline
        red.srem(rids, rid) # We are done with rid, remove it
        if success:
            msg = ('Action "{}" ran successfully against ({}): {} => {}')
            logging.info(msg.format(action_name, rid, rec, result))

##############################################################################

def main():
    'Parse args, then start reading queue forever.'
    possible_qnames = ['transfer', 'submit']
    parser = argparse.ArgumentParser(
        description='Data Queue service',
        epilog='EXAMPLE: %(prog)s --loglevel DEBUG &'
        )

    parser.add_argument('--cfg',
                        help='Configuration file (json format)',
                        type=argparse.FileType('r'))
    parser.add_argument('--logconf',
                        help='Logging configuration file (YAML format)',
                        default='/etc/tada/pop.yaml',
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
    #!logging.basicConfig(level=log_level,
    #!                    format='%(levelname)s %(message)s',
    #!                    datefmt='%m-%d %H:%M')
    logDict = yaml.load(args.logconf)
    print('logDict={}'.format(logDict), flush=True)
    logging.config.dictConfig(logDict)
    logging.getLogger().setLevel(log_level)

    logging.debug('\nDebug output is enabled!!')
    ###########################################################################


    qcfg, dirs = config.get_config(possible_qnames)
    dqutils.save_pid(sys.argv[0], piddir=dirs['run_dir'])
    process_queue_forever(args.queue, qcfg, dirs)

if __name__ == '__main__':
    main()

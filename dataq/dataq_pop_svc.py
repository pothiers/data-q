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

from tada import config
from . import dqutils as du
from . import red_utils as ru
from .dbvars import *
from .actions import *

# GROSS: highly nested code!!!
def process_queue_forever(qname, qcfg, dirs, delay=1.0):
    'Block waiting for items on queue, then process, repeat.'
    red = ru.redis_protocol()
    action_name = qcfg[qname]['action_name']
    action = action_lut[action_name]
    maxerrors = qcfg[qname]['maximum_errors_per_record']

    logging.debug('Read Queue "{}"'.format(qname))
    while True: # pop from queue forever
        logging.debug('Read Queue: loop')

        if not ru.action_p(red):
            time.sleep(delay)
            continue

        rid = ru.next_record(red)
        if rid == None:
            continue
        logging.debug('Read Queue: got "{}"'.format(rid))


        rec = du.decode_dict(red.hgetall(rid))
        if len(rec) == 0:
            raise Exception('No record found for rid={}'
                            .format(rid))

        error_count = ru.get_error_count(red,rid)
        success = True
        
        # buffer all commands done by pipeline, make command list atomic
        with red.pipeline() as pl:
            try:
                # switch to normal pipeline mode where commands get buffered
                pl.multi()
                #!pl.srem(aqs, rid)
                ru.activeq_remove(pl, rid)
                try:
                    logging.debug('RUN action: "{}"; {}"'
                                  .format(action_name, rec))
                    result = action(rec, qname, qcfg=qcfg, dirs=dirs)
                    logging.debug('DONE action: "{}" => {}'
                                  .format(action_name, result))
                except Exception as ex:
                    # action failed
                    success = False
                    logging.debug('Action "{}" failed: {}; {}'
                                  .format(action_name,
                                          ex,
                                          du.trace_str()))
                    pl.hincrby(ecnt, rid)
                    error_count += 1
                    logging.debug('dbg-1')
                    # pl.hget() returns StrictPipeline; NOT value of key!
                    # use of RED here causes us to not get incremented value
                    logging.debug('dbg-2')
                    logging.debug('Error(#{}) running action "{}"'
                                  .format(error_count, action_name))
                    if error_count > maxerrors:
                        msg = ('Failed to run action "{}" {} times. '
                               +' Max allowed is {} so moving it to the'
                               +' INACTIVE queue.'
                               +' Record={}. Exception={}')
                        logging.error(msg.format(action_name,
                                                 error_count, maxerrors,
                                                 rec, ex))
                        # action kept failing: move to Inactive queue
                        pl.lpush(iq, rid)  
                    else:
                        msg = ('Failed to run action "{}" {} times. '
                               +' Max allowed is {} so will try again later.'
                               +' Record={}. Exception={}')
                        logging.error(msg.format(action_name,
                                                 cnt, maxerrors, rec, ex))
                        # failed: go to the end of the line
                        pl.lpush(aq, rid) 
                #!pl.srem(rids, rid)
                pl.execute() # execute the pipeline
            except Exception as err:
                success = False
                pl.lpush(iq, rid)  
                logging.error('Unexpected exception; {}; {}'
                              .format(err,du.trace_str()))
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

    #!parser.add_argument('--cfg',
    #!                    help='Configuration file (json format)',
    #!                    type=argparse.FileType('r'))
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
    logging.basicConfig(level=log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M')
    logging.debug('\nDebug output is enabled!!')

    logDict = yaml.load(args.logconf)
    print('logDict={}'.format(logDict), flush=True)
    logging.config.dictConfig(logDict)
    logging.getLogger().setLevel(log_level)

    ###########################################################################

    qcfg, dirs = config.get_config(possible_qnames)
    du.save_pid(sys.argv[0], piddir=dirs['run_dir'])
    process_queue_forever(args.queue, qcfg, dirs)

if __name__ == '__main__':
    main()

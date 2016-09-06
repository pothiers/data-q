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
import time
import sys
import traceback
import yaml
from datetime import datetime
import shutil


from tada import config
from tada import tada_utils as tut
from . import dqutils as du
from . import red_utils as ru
from .dbvars import *
from .actions import *

msghi = ('Failed to run action "{}" {} times. '
                   +' Max allowed is {} so moving it to the'
                   +' INACTIVE queue. Record={}.')
msglo = ('Failed to run action "{}" {} times. '
         'Max allowed is {} so will try again later.'
         +' Record={}.')


def logheartbeat(red):
    logname='/var/log/tada/dqpop-heartbeat.log'
    try:
        logheartbeat.counter += 1
        timestamp = str(datetime.now())
        logging.debug('dataq-pop heartbeat[{}]: {}'
                      .format(logheartbeat.counter, timestamp))
        lenActive = red.llen(aq)
        with open(logname, 'a') as f:
            # log: <timestamp> <free disk space in bytes>
            freemb = int(shutil.disk_usage('/var/tada/').free / 1E6)
            print(timestamp, lenActive, freemb, 'avail-mb', file=f)
    except:
        pass

def process_queue_forever(qname, qcfg, dirs, delay=1.0):
    'Block waiting for items on queue, then process, repeat.'
    red = ru.redis_protocol()
    action_name = qcfg[qname]['action_name']
    action = action_lut[action_name]
    maxerrors = qcfg['maximum_errors_per_record']
    logheartbeat.counter = 0
    logheartbeat(red)
    hiera = tut.read_hiera_yaml()
    # seconds to wait before unblocking
    timeout = hiera.get('dq_unblock_timeout', 0) 

    #! logging.debug('Read Queue "{}"'.format(qname))
    while True: # pop from queue forever
        logging.debug('Read Queue: loop (blocking for NEXT RECORD or {} secs)'
                      .format(timeout))
        rid = ru.next_record(red, timeout=timeout) # BLOCKING
        if rid == None:
            logging.debug('Read Queue: rid == None. '
                          'Should only happen on toggle of ACTION flag'
                          ' or timeout.')
            continue
        logheartbeat(red)

        rec = ru.get_record(red, rid)
        if len(rec) == 0:
            raise Exception('No record found for rid={}'.format(rid))
        #!logging.debug('Read Queue: id={}; {}'.format(rid,rec))

        error_count = ru.get_error_count(red, rid)
        success = True
        try:
            logging.debug('RUN action: {}'.format(action_name))
            result = action(rec, qname, qcfg=qcfg, dirs=dirs)
            success = result
            if success == False:
                error_count += 1
                ru.incr_error_count(red, rid)
            logging.debug('Action passed: "{}"({}) => {}'
                          .format(action_name,
                                  rec.get('filename','NA'),
                                  result))
        except Exception as ex:
            # action failed
            success = False
            error_count += 1
            ru.incr_error_count(red, rid)
            #! ru.force_save(red) #!!! remove (diag)
            #! ru.log_queue_record(red, rid, msg='DBG2.12b ')
            # DBG: the dictionary associated with RID is present here.
            
            msg = msghi if (error_count > maxerrors) else msglo
            logging.debug(msg.format(action_name, error_count, maxerrors, rec))

            # Dbg: The Dictionary Associated with RID diappeared here.
            ru.set_record(red, rid, rec) #should not be necessary!!!

            logging.error('Error running {} action. error:{}; trace:{}'
                          .format(action_name.upper(), ex, du.trace_str()))
            #sys.exit(ex)

        # buffer all commands done by pipeline, make command list atomic
        with red.pipeline() as pl:
            try:
                # switch to normal pipeline mode where commands get buffered
                pl.multi()
                if success == False:
                    if error_count > maxerrors:
                        # action kept failing: move to Inactive queue
                        #! ru.push_to_inactive(pl, rid)
                        pass
                    else:
                        # failed: go to the end of the line
                        ru.push_to_active(pl, rid)
                pl.execute() # execute the pipeline
            except Exception as err:
                success = False
                #!ru.push_to_inactive(pl, rid)
                logging.error('Unexpected exception; {}; {}'
                              .format(err,du.trace_str()))
                pl.execute() # execute the pipeline
        # END with pipeline
        if success:
            ru.remove_record(red, rid)  # We are done with rid, remove it
            msg = ('Action "{}" ran successfully against ({}): {} => {}')
            logging.debug(msg.format(action_name,
                                     rid,
                                     rec.get('filename','NA'),
                                     result))
        #!ru.log_queue_summary(red)
        #!ru.log_queue_record(red, rid, msg='success={} '.format(success))
    # END while true
    
##############################################################################

def main():
    'Parse args, then start reading queue forever.'
    possible_qnames = ['transfer', 'submit']
    logconf='/etc/tada/pop.yaml'
    parser = argparse.ArgumentParser(
        description='Data Queue service',
        epilog='EXAMPLE: %(prog)s --loglevel DEBUG &'
        )

    #!parser.add_argument('--cfg',
    #!                    help='Configuration file (json format)',
    #!                    type=argparse.FileType('r'))
    parser.add_argument('--logconf',
                        help='Logging configuration file (YAML format).'
                        '[Default={}]'.format(logconf),
                        default=logconf,
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
    #! logging.debug('\nDebug output is enabled!!')

    logDict = yaml.load(args.logconf)
    logging.config.dictConfig(logDict)
    logging.getLogger().setLevel(log_level)

    ###########################################################################

    qcfg, dirs = config.get_config(possible_qnames)
    logging.info('logDict={}'.format(logDict))
    logging.info('DATAQ started: {}'.format(datetime.now().isoformat()))
    logging.info('Tada-Config content({}): {}'.format(args.queue, qcfg))

    du.save_pid(sys.argv[0], piddir='/var/run/tada')
    process_queue_forever(args.queue, qcfg, dirs)

if __name__ == '__main__':
    main()

#! /usr/bin/env python
'''\ 
Read data records from socket and push to queue. 
Pop records from queue and apply action. 

The checksum provided for each data record is used as an ID.  If the
checksum of two records is the same, we assume the data is. So we 
throw one of them away.
'''

import os
import sys
import string
import argparse
import logging
import json
import time
import random
import socketserver
import asyncio

import redis

from dbvars import *
from actions import *
import utils

@asyncio.coroutine
def pushq(r, host, port, delay=1):
    if r.get(readP) == 'off':
        return false
    reader,writer = yield from asyncio.open_connection(host,port)
    while True:
        data = reader.readline()
        (fname,checksum,size) = data.split() # specific to our APP!!!
        rec = dict(list(zip(['filename','size'],[fname,size]))) 

        pl = r.pipeline()
        pl.watch(rids,aq,aqs,checksum)
        pl.multi()
        if r.sismember(aqs,checksum) == 1:
            logging.warning(': Record for %s is already in queue.' 
                            +' Ignoring duplicate.', checksum)
            writer.write('Ignored ID=%s'%checksum)
        else:
            # add to DB
            pl.sadd(aqs,checksum) 
            pl.lpush(aq,checksum)
            pl.sadd(rids,checksum) 
            pl.hmset(checksum,rec)
            pl.save()    
            writer.write(bytes('Pushed ID=%s'%checksum,'UTF-8'))
        pl.execute()
        yield from asyncio.sleep(delay)

@asyncio.coroutine
def popq(r, action, delay=1):
    errorCnt = 0
    logging.debug('Process Queue')
    while True:
        if r.get(actionP) == b'off':
            yield from asyncio.sleep(delay)
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
        yield from asyncio.sleep(delay)

    

##############################################################################
def main():
    #! print('EXECUTING: %s\n\n' % (string.join(sys.argv)))
    parser = argparse.ArgumentParser(
        description='Read data from socket and push to Data Queue',
        epilog='EXAMPLE: %(prog)s --host localhost --port 9988'
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
    ######################################################################

    #! server = socketserver.TCPServer((args.host, args.port),
    #!                                 DataRecordTCPHandler)
    #! 
    #! # Activate the server; this will keep running until you
    #! # interrupt the program with Ctrl-C
    #! server.r = redis.StrictRedis()
    #! server.serve_forever()

    if args.cfg is None:
        cfg = dict(
            maximum_errors_per_record = 0, 
            action_name = "echo00",
            )
    else:
        cfg = json.load(cfg_file)
    action_name = cfg['action_name']
    action = action_lut[action_name]

    r = redis.StrictRedis()
    loop = asyncio.get_event_loop()
    tasks = [
            loop.create_task(pushq(r, args.host, args.port)),
            loop.create_task(popq(r,action)),
            ]
    #!loop.run_forever()
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

if __name__ == '__main__':
    main()

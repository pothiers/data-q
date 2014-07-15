#! /usr/bin/env python
'''\ 

Get data record from socket and push to queue. Pop and apply
action. Be resilient.

The checksum provided for each data record is used as an ID.  If the
checksum of two records is the same, we assume the data is. So we can
throw one of them away.

To tell if two records are identical, we check equality of ALL three of: 
   checksum
   full file name
   file size

TODO:
- regularly tell Redis to save to disk
- setup as daemon
- trap for everything bad and do something good
'''

import os, sys, string, argparse, logging
from pprint import pprint 
import random
import redis
import SocketServer

aq = 'activeq' # Active Queue. List of IDs. Pop and apply actions from this.
iq = 'inactiveq' # List of IDs. Stash records that will not be popped here
ecnt = 'errorcnt' # errorcnt[id] = cnt; number of Action errors against ID

def echo(rec, probFail = 0.10):
    print('Processing file: %s' % rec['filename'])
    # !!! randomize success to simulate errors on cmds
    return random.random() > probFail


# !!! Temp config.  Will move external later. (ConfigParser)
cfg = dict(
    actionName = 'echo',
    action = echo,
    )



class DataRecordTCPHandler(SocketServer.StreamRequestHandler):

    def handle0(self):
        # self.rfile is a file-like object created by the handler;
        # we can now use e.g. readline() instead of raw recv() calls
        self.data = self.rfile.readline().strip()
        print "{} wrote:".format(self.client_address[0])
        print self.data
        # Likewise, self.wfile is a file-like object used to write back
        # to the client
        self.wfile.write(self.data.upper())

    def handle(self):
        r = self.server.r
        activeIds = self.server.activeIds
        lut = self.server.lut
        print 'server.r=',r
        self.data = self.rfile.readline().strip()
        
        (fname,checksum,size) = self.data.split() #! specific to our APP
        if checksum in activeIds:
            logging.warning(': Record for %s is already in queue.' 
                            +' Ignoring duplicate.', checksum)
            warnings += 1
            return False
                
        activeIds.add(checksum)
        rec = dict(list(zip(['filename','size'],[fname,int(size)])))
        lut[checksum] = rec
        
        # add to DB
        r.lpush(aq,checksum)
        r.hmset(checksum,rec)

        print("{} wrote:".format(self.client_address[0]))
        print(self.data)
        self.wfile.write(self.data.upper())

    
        
def processQueue(maxErrPer=3): 
    r = redis.StrictRedis()

    errorCnt = 0
    print('Process Queue')
    while r.llen(aq) > 0:
        rid = r.rpop(aq)
        rec = r.hgetall(rid)
        success = cfg['action'](rec)
        if not success:
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

def main_tt():
    cmd = 'MyProgram.py foo1 foo2'
    sys.argv = cmd.split()
    res = main()
    return res

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


    # Create the server, binding to HOST on PORT
    server = SocketServer.TCPServer((args.host, args.port),
                                    DataRecordTCPHandler)
    # stuff local structures from DB
    r = redis.StrictRedis()
    activeIds = set(r.lrange(aq,0,-1))
    lut = dict() # lut[rid] => dict(filename,checksum,size)
    for rid in activeIds:
        lut[rid] = r.hgetall(rid)
    server.r = r
    server.lut = lut
    server.activeIds = activeIds


    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()

if __name__ == '__main__':
    main()

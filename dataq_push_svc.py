#! /usr/bin/env python
'''\ 
Read data records from socket and push to queue. 

The checksum provided for each data record is used as an ID.  If the
checksum of two records is the same, we assume the data is. So we can
throw one of them away.

TODO:
- setup as daemon
- trap for everything bad and do something good
'''

import os, sys, string, argparse, logging
import random
import redis
import socketserver
from dbvars import *

class DataRecordTCPHandler(socketserver.StreamRequestHandler):

    def handle(self):
        r = self.server.r
        if r.get(readP) == 'off':
            return False

        self.data = self.rfile.readline().strip().decode()
        logging.debug('Data line read from socket=%s',self.data)
        (fname,checksum,size) = self.data.split() #! specific to our APP
        rec = dict(list(zip(['filename','size'],[fname,size])))

        pl = r.pipeline()
        pl.watch(rids,aq,aqs,checksum)
        pl.multi()
        if r.sismember(aqs,checksum) == 1:
            logging.warning(': Record for %s is already in queue.' 
                            +' Ignoring duplicate.', checksum)
            self.wfile.write('Ignored ID=%s'%checksum)
        else:
            # add to DB
            pl.sadd(aqs,checksum) 
            pl.lpush(aq,checksum)
            pl.sadd(rids,checksum) 
            pl.hmset(checksum,rec)
            pl.save()    
            self.wfile.write(bytes('Pushed ID=%s'%checksum,'UTF-8'))
        pl.execute()

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

    server = socketserver.TCPServer((args.host, args.port),
                                    DataRecordTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.r = redis.StrictRedis()
    server.serve_forever()

if __name__ == '__main__':
    main()

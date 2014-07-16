#! /usr/bin/env python
'''\ 
Read data records from socket and push to queue. 

The checksum provided for each data record is used as an ID.  If the
checksum of two records is the same, we assume the data is. So we can
throw one of them away.

TODO:
- regularly tell Redis to save to disk
- setup as daemon
- trap for everything bad and do something good
'''

import os, sys, string, argparse, logging
import random
import redis
import SocketServer

aq = 'activeq' # Active Queue. List of IDs. Pop and apply actions from this.
iq = 'inactiveq' # List of IDs. Stash records that will not be popped here
ecnt = 'errorcnt' # errorcnt[id] = cnt; number of Action errors against ID
actionP = 'actionFlag' # on|off
readP = 'readFlag' # on|off

class DataRecordTCPHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        lut = dict() # lut[rid] => dict(filename,checksum,size,prio)
        r = self.server.r

        if r.get(readP) == 'off':
            return False

        activeIds = set(r.lrange(aq,0,-1))
        for rid in activeIds:
            lut[rid] = r.hgetall(rid)

        self.data = self.rfile.readline().strip()
        
        (fname,checksum,size) = self.data.split() #! specific to our APP
        if checksum in activeIds:
            logging.warning(': Record for %s is already in queue.' 
                            +' Ignoring duplicate.', checksum)
        else:
            activeIds.add(checksum)
            rec = dict(list(zip(['filename','size'],[fname,int(size)])))
            lut[checksum] = rec
            
            # add to DB
            r.lpush(aq,checksum)
            r.hmset(checksum,rec)
    
            print("{} wrote:".format(self.client_address[0]))
            print(self.data)
            self.wfile.write(self.data.upper())

        

        


##############################################################################
def main():
    print('EXECUTING: %s\n\n' % (string.join(sys.argv)))
    parser = argparse.ArgumentParser(
        version='1.0.3',
        description='Read data from socket and push to Data Queue',
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


    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.r = redis.StrictRedis()
    server.serve_forever()

if __name__ == '__main__':
    main()

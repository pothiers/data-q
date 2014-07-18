#! /usr/bin/env python
'''\
Read a file of data records, send to dataq_push_svc over socket.
'''

import os, sys, string, argparse, logging
import socket
import time
import random


def feedFile(infile, host, port):
    for line in infile:
        delay = random.triangular(0,2.0,0.5)
        data = line.strip()
        logging.info('Sending (%s): %s ',delay,data)

        # Create a socket (SOCK_STREAM means a TCP socket)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            time.sleep(delay)
            # Connect to server and send data
            sock.connect((host, port))
            sock.sendall(data+'\n')

            # Receive data from the server
            received = sock.recv(1024)
        finally:
            # ... and shut down
            sock.close()

        print "Received: {}".format(received)



##############################################################################

def main_tt():
    cmd = 'MyProgram.py foo1 foo2'
    sys.argv = cmd.split()
    res = main()
    return res

def main():
    #!print 'EXECUTING: %s\n\n' % (string.join(sys.argv))
    parser = argparse.ArgumentParser(
        version='1.0.1',
        description='My shiny new python program',
        epilog='EXAMPLE: %(prog)s a b"'
        )
    parser.add_argument('infile',  help='Input file',
                        type=argparse.FileType('r') )
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
    logging.debug('Debug output is enabled by test_feeder !!!')
    ######################################################################

    feedFile(args.infile, args.host, args.port)

if __name__ == '__main__':
    main()

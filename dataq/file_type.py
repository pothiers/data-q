#! /usr/bin/env python
"Intended for use by irods iexecmd.  Determines type of a file stored in irods"

import magic
import os, sys, string, argparse, logging


# +++ Add code here if TADA needs to handle additional types of files!!!
def file_type(filename, ifname):
    """For use by irods iexecmd. Output an abstracted file type string.
 MIME isn't always good enough."""
    if magic.from_file(filename).decode().find('FITS image data') >= 0:
        print('FITS')
    elif magic.from_file(filename).decode().find('JPEG image data') >= 0:
        print('JPEG')
    elif magic.from_file(filename).decode().find('script text executable') >= 0:
        print('shell script')
    else:
        print('UNKNOWN')


##############################################################################

def main():
    parser = argparse.ArgumentParser(
        description='My shiny new python program',
        epilog='EXAMPLE: %(prog)s filename"'
        )
    parser.add_argument('infile',  help='Input file',
                        type=argparse.FileType('r') )
    parser.add_argument('irods_name',
                        help='Full path in iRODS',
                        )

    parser.add_argument('--loglevel',      help='Kind of diagnostic output',
                        choices = ['CRTICAL','ERROR','WARNING','INFO','DEBUG'],
                        default='WARNING',
                        )
    args = parser.parse_args()
    args.infile.close()
    args.infile = args.infile.name

    #!print 'My args=',args
    #!print 'infile=',args.infile

    log_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(log_level, int):
        parser.error('Invalid log level: %s' % args.loglevel) 
    logging.basicConfig(level = log_level,
                        format='%(levelname)s %(message)s',
                        datefmt='%m-%d %H:%M'
                        )
    logging.debug('Debug output is enabled by nitfConvert!!!')


    file_type(args.infile, args.irods_name)

if __name__ == '__main__':
    main()

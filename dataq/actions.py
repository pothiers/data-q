"Actions that can be run against entry when popped off queue."
import random
import logging
import sys

import os, os.path
import socket
import tada.submit
import magic

from . import irods_utils as iu
from . import dqutils as du
from . import dataq_cli as dqc

def echo00(rec, **kwargs):
    "For diagnostics (never fails)"
    prop_fail = 0.00
    print('Action=echo00: rec={} kwargs={}'.format(rec, kwargs))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def echo30(rec, **kwargs):
    "For diagnostics (fails 30% of the time)"
    prop_fail = 0.30
    print('Action=echo30: rec={} kwargs={}'.format(rec, kwargs))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def push_to_q(dq_host, dq_port, fname, checksum):
    'Push a line onto data-queue named by qname.'
    logging.debug('push_to_q({}, {}, {})'.format(dq_host, dq_port, fname))
    data = '{} {}\n'.format(checksum, fname)
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect to server and send data
        sock.connect((dq_host, dq_port))
        sock.sendall(bytes(data, 'UTF-8'))

        # Receive data from the server and shut down
        received = sock.recv(1024)
    finally:
        sock.close()


def network_move(rec, **kwargs):
    "Transfer from Mountain to Valley"
    logging.debug('Transfer from Mountain to Valley.')
    if 'qcfg' not in kwargs:
        raise Exception(
            'ERROR: "network_move" Action did not get required '
            +' keyword parameter: "{}" in: {}'
            .format('qcfg', kwargs))
    qcfg=kwargs['qcfg']

    dq_host = qcfg['submit']['dq_host']
    dq_port = qcfg['submit']['dq_port']

    #!source_root = kwargs['source_root']
    #!irods_root = kwargs['irods_root']  # eg. '/tempZone/valley/'
    source_root = '/var/tada/mountain_cache/'  #!!!
    irods_root = '/tempZone/valley/mountain_mirror/'  #!!!
    fname = rec['filename']            # absolute path

    logging.debug('source_root={}, fname={}'.format(source_root, fname))
    if fname.index(source_root) != 0:
        raise Exception('Filename "{}" does not start with "{}"'
                        .format(fname, source_root))

    ifname = os.path.join(irods_root, os.path.relpath(fname, source_root))

    try:
        iu.irods_put(fname, ifname)
    except:
        logging.warning('Failed to transfer from Mountain to Valley.')
        # Any failure means put back on queue. Keep queue handling
        # outside of actions where possible.
        raise
    else:
        # successfully transfered to Valley
        os.remove(fname)
        logging.info('Removed file "%s" from mountain cache'%(rec['filename'],))
        push_to_q(dq_host, dq_port, ifname, rec['checksum'])
    return True



def submit(rec, **kwargs):
    "Try to modify headers and submit FITS to archive; or push to Mitigate"
    import redis
    logging.info('Try to submit to archive. ({}) file={}'
                 .format(rec['error_count'], rec['filename']))
    qcfg = du.get_keyword('qcfg', kwargs)
    dq_host = qcfg['mitigate']['dq_host']
    dq_port = qcfg['mitigate']['dq_port']

    #!archive_root = kwargs['archive_root']
    #!noarc_root = kwargs['noarchive_root']
    #!mitag_root = kwargs['mitigate_root']
    #!irods_root = kwargs['irods_root']  # eg. '/tempZone/valley/'
    archive_root = '/var/tada/archive/'  #!!!
    noarc_root = '/var/tada/no-archive/' #!!!
    mitag_root = '/var/tada/mitigate/'   #!!!
    irods_root = '/tempZone/valley/mountain_mirror/'  #!!!

    # eg. /tempZone/valley/mountain_mirror/other/vagrant/16/text/plain/fubar.txt
    ifname = rec['filename']            # absolute path
    tail = os.path.relpath(ifname, irods_root) # changing part of path tail

    ##
    ## Put irods file on filesystem. We might mv it later.
    ##
    fname = os.path.join(noarc_root, tail)
    try:
        iu.irods_get(fname, ifname)
    except:
        logging.warning('Failed to get file from irods on Valley.')
        raise

    new_fname = None
    if magic.from_file(fname).decode().find('FITS image data') < 0:
        # not FITS
        # Remove files if noarc_root is taking up too much space (FIFO)!!!
        logging.debug('Non-fits file put in: {}'.format(fname))
    else:
        # is FITS
        fname = du.move(noarc_root, fname, archive_root)
        try:
            fname = tada.submit.submit_to_archive(fname, archive_root)
            logging.debug('Calculated fname: {}'.format(fname))
        except:
            # We should really do several automatic re-submits first!!!
            logging.error(
                'Failed submit_to_archive({}). Pushing to Mitigation'
                .format(fname))
            # move NOARC to MITIG directory
            mfname = os.path.join(mitag_root, tail)
            #!os.rename(fname, mfname)
            du.move(archive_root, fname, mitag_root, os.path.basename(fname))
            logging.debug('Moved to: {}'.format(mfname))

            # deactivate!!!
            # see dataq_cli.py:deactivate_range()
            red = redis.StrictRedis()
            dqc.deactivate_range(red, rec['checksum'], rec['checksum'])
            logging.debug('Deactivated {}'.format(mfname))

            # Push to queue that operator should monitor.
            #push_to_q(dq_host, dq_port, mfname, rec['checksum']) !!! 9989
#!        else:
#!            afname = os.path.join(archive_root, tail)
#!            dest = du.move(noarc_root, fname, archive_root, new_fname)
#!            logging.debug('Moved file to: {}'.format(dest))

    return True
# END submit() action

def mitigate(rec, **kwargs):
    pass

action_lut = dict(
    echo00 = echo00,
    echo30 = echo30,
    network_move = network_move,
    submit = submit,
    mitigate = mitigate,
    )

        

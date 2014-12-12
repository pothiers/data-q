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

def echo00(rec, qname, **kwargs):
    "For diagnostics (never fails)"
    prop_fail = 0.00
    print('[{}] Action=echo00: rec={} kwargs={}'.format(qname, rec, kwargs))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def echo30(rec, qname, **kwargs):
    "For diagnostics (fails 30% of the time)"
    prop_fail = 0.30
    print('[{}] Action=echo30: rec={} kwargs={}'.format(qname, rec, kwargs))
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


def network_move(rec, qname, **kwargs):
    "Transfer from Mountain to Valley"
    logging.debug('ACTION: network_move()')
    for p in ['qcfg', 'dirs']:
        if p not in kwargs:
            raise Exception(
                'ERROR: "network_move" Action did not get required '
                +' keyword parameter: "{}" in: {}'
                .format(p, kwargs))
    qcfg=kwargs['qcfg']
    dirs=kwargs['dirs']
    logging.debug('dirs={}'.format(dirs))

    nextq = qcfg['transfer']['next_queue']
    dq_host = qcfg[nextq]['dq_host']
    dq_port = qcfg[nextq]['dq_port']

    #!irods_root = kwargs['irods_root']  # eg. '/tempZone/'
    source_root = qcfg['transfer']['cache_dir']
    irods_root = qcfg['transfer']['mirror_irods']
    fname = rec['filename']            # absolute path

    logging.debug('source_root={}, fname={}'.format(source_root, fname))
    if fname.index(source_root) != 0:
        raise Exception('Filename "{}" does not start with "{}"'
                        .format(fname, source_root))

    ifname = os.path.join(irods_root, os.path.relpath(fname, source_root))

    try:
        iu.irods_put(fname, ifname)
    except Exception as ex:
        logging.warning('Failed to transfer from Mountain to Valley. {}'
                        .format(ex))
        # Any failure means put back on queue. Keep queue handling
        # outside of actions where possible.
        raise
    else:
        # successfully transfered to Valley
        os.remove(fname)
        logging.info('Removed file "%s" from mountain cache'%(rec['filename'],))
        push_to_q(dq_host, dq_port, ifname, rec['checksum'])
    return True



def submit(rec, qname, **kwargs):
    "Try to modify headers and submit FITS to archive; or push to Mitigate"
    logging.debug('ACTION: submit()')
    import redis
    qcfg = du.get_keyword('qcfg', kwargs)
    dq_host = qcfg[qname]['dq_host']
    dq_port = qcfg[qname]['dq_port']

    archive_root = qcfg[qname]['archive_dir']
    noarc_root =  qcfg[qname]['noarchive_dir']
    irods_root =  qcfg[qname]['mirror_irods'] # '/tempZone/mountain_mirror/'
    #! mitag_root = '/var/tada/mitigate/' 

    # eg. /tempZone/mountain_mirror/other/vagrant/16/text/plain/fubar.txt
    ifname = rec['filename']            # absolute path
    checksum = rec['checksum']          
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
        logging.info('Non-fits file put in: {}'.format(fname))
    else:
        # is FITS
        fname = du.move(noarc_root, fname, archive_root)
        try:
            fname = tada.submit.submit_to_archive(fname, checksum, qname, qcfg)
        except Exception as sex:
            raise sex
        else:
            logging.info('PASSED submit_to_archive({}).'  .format(fname))

    return True
# END submit() action

def mitigate(rec, qname, **kwargs):
    pass

action_lut = dict(
    echo00=echo00,
    echo30=echo30,
    network_move=network_move,
    submit=submit,
    mitigate=mitigate,
    )

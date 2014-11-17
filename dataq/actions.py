"Actions that can be run against entry when popped off queue."
import random
import logging
import sys

import os, os.path
import subprocess
import socket
import tada.submit
import magic

def echo00(rec, **kwargs):
    prop_fail = 0.00
    "For diagnostics (never fails)"
    print(('Action=echo00: processing record: %s' % rec))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def echo10(rec, **kwargs):
    prop_fail = 0.10
    "For diagnostics (fails 10% of the time)"
    print(('Action=echo10: processing record: %s' % rec))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def echo30(rec, **kwargs):
    prop_fail = 0.30
    "For diagnostics (fails 30% of the time)"
    print(('Action=echo30: processing record: %s' % rec))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def push_to_q(dq_host, dq_port, fname, checksum):
    'Push a line onto data-queue named by qname.'
    data = '{} {}\n'.format(checksum, fname)
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("DBG: connect to {}:{}".format(dq_host, dq_port))

    try:
        print("DBG: Send: {}".format(data))

        # Connect to server and send data
        sock.connect((dq_host, dq_port))
        sock.sendall(bytes(data, 'UTF-8'))

        # Receive data from the server and shut down
        received = sock.recv(1024)
    finally:
        sock.close()

    print("DBG: Received: {}".format(received))

    
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
    
    ifname = os.path.join(irods_root,fname[len(source_root):])
    cmdargs1 = ['imkdir', '-p', os.path.dirname(ifname)]
    cmdargs2 = ['iput', '-f', fname, ifname]

    try:
        logging.info('Removed file "%s" from mountain cache'%(rec['filename'],))
        #!icmds.iput('-f', rec['filename'], irods_path)
        subprocess.check_output(cmdargs1)
        subprocess.check_output(cmdargs2)
    except subprocess.CalledProcessError as e:
        logging.warning('Failed using irods.iput() to transfer'
                        +' from Mountain to Valley.')
        print('Execution failed: ', e, file=sys.stderr)
        # Any failure means put back on queue. Keep queue handling
        # outside of actions where possible.
        raise
    else:
        # successfully transfered to Valley
        os.remove(fname)
        logging.info('Removed file "%s" from mountain cache'%(rec['filename'],))
        kwargs
        push_to_q(dq_host, dq_port, ifname, rec['checksum'])
    return True



def submit(rec, **kwargs):
    "Try to modify headers and submit FITS to archive; or push to Mitigate"
    logging.debug('Submit to archive (if we can).')
    if 'qcfg' not in kwargs:
        raise Exception(
            'ERROR: "submit" Action did not get required '
            +' keyword parameter: "{}" in: {}'
            .format('qcfg', kwargs))
    qcfg=kwargs['qcfg']
    
    dq_host = qcfg['mitigate']['dq_host']
    dq_port = qcfg['mitigate']['dq_port']

    #!noarc_root = kwargs['noarchive_root']
    #!irods_root = kwargs['irods_root']  # eg. '/tempZone/valley/'
    noarc_root = '/var/tada/no-archive/'  #!!!
    irods_root = '/tempZone/valley/mountain_mirror/'  #!!!

    # eg. /tempZone/valley/mountain_mirror/other/vagrant/16/text/plain/fubar.txt
    ifname = rec['filename']            # absolute path

    if ifname.index(irods_root) != 0:
        raise Exception('iFilename "{}" does not start with "{}"'
                        .format(ifname, irods_root))

    ##
    ## Put irods file on filesystem. We might mv it later.
    ##
    fname = os.path.join(noarc_root, ifname[len(irods_root):])
    cmdargs1 = ['mkdir', '-p', os.path.dirname(fname)]
    cmdargs2 = ['iget', '-f', ifname, fname]
    try:
        subprocess.check_output(cmdargs1)
        subprocess.check_output(cmdargs2)
    except subprocess.CalledProcessError as e:
        logging.warning('Failed using irods.iget() on Valley.')
        print('Execution failed: ', e, file=sys.stderr)
        raise
    

    if magic.from_file(fname).decode().find('FITS image data') < 0:
        # not FITS
        # Remove files if noarc_root is taking up too much space (FIFO)!!!
        logging.debug('Non-fits file put in: {}'.format(fname))
    else:
        # is FITS
        try:
            tada.submit.submit_to_archive(fname)
        except Exception as e:
            logging.error('Failed submit_to_archive("{}").'
                          + ' Pushing to Mitigation'
                          .format(fname))
            # move fname to mitig directory !!!
            # mitigate.push(fname) !!!
            raise

    return True

def mitigate(rec, **kwargs):
    pass

action_lut = dict(
    echo00 = echo00,
    echo10 = echo10,
    echo30 = echo30,
    network_move = network_move,
    submit = submit,
    mitigate = mitigate,
    )

        

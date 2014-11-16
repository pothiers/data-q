"Actions that can be run against entry when popped off queue."
import random
import logging
import sys

import os, os.path
import subprocess

def echo00(rec, prop_fail = 0.00):
    "For diagnostics (never fails)"
    print(('Action=echo00: processing record: %s' % rec))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def echo10(rec, prop_fail = 0.10):
    "For diagnostics (fails 10% of the time)"
    print(('Action=echo10: processing record: %s' % rec))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def echo30(rec, prop_fail = 0.30):
    "For diagnostics (fails 30% of the time)"
    print(('Action=echo30: processing record: %s' % rec))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail

def network_move(rec, **kwargs):
    "Transfer from Mountain to Valley"
    logging.debug('Transfer from Mountain to Valley.')
    
    #!source_root = kwargs['source_root']
    #!irods_root = kwargs['irods_root']  # eg. '/tempZone/valley/'
    source_root = '/var/tada/mountain_cache/'  #!!!
    irods_root = '/tempZone/valley/mountain_mirror/'  #!!!
    fname = rec['filename']            # absolute path
    assert fname.index(source_root) == 0
    
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
        submit_q.push(rec)
    return True

def disk_register(rec): # disk storage
    pass

def archive_ingest(rec):
    pass


action_lut = dict(
    echo00 = echo00,
    echo10 = echo10,
    echo30 = echo30,
    network_move = network_move,
    disk_register = disk_register,
    archive_ingest = archive_ingest,
    )

        

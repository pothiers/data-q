"Actions that can be run against entry when popped off queue."
import random
import logging

# Possible actions that can be performed upon pop of record from queue.

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

def network_move(rec):
    "Transfer from Mountain to Valley"
    try:
        iput(rec['filename'], irods_path)
    except:
        # Any failure means put back on queue. Keep queue handling
        # outside of actions where possible.
        raise
    else:
        # successfully transfered to Valley
        os.remove(rec['filename'])
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

        

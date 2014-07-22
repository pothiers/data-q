import random

# Possible actions that can be performed upon pop of record from queue.

def echo00(rec, probFail = 0.00):
    print('Action=echo00: processing record: %s' % rec)
    # randomize success to simulate errors on cmds
    return random.random() >= probFail

def echo10(rec, probFail = 0.10):
    print('Action=echo10: processing record: %s' % rec)
    # randomize success to simulate errors on cmds
    return random.random() >= probFail

def echo30(rec, probFail = 0.30):
    print('Action=echo30: processing record: %s' % rec)
    # randomize success to simulate errors on cmds
    return random.random() >= probFail

def network_move(rec):
    pass

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

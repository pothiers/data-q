"Actions that can be run against entry when popped off queue."
import random
import logging
import tada.actions

def echo30(rec, qname, **kwargs):
    "For diagnostics (fails 30% of the time)"
    prop_fail = 0.30
    print('[{}] Action=echo30: rec={} kwargs={}'.format(qname, rec, kwargs))
    # randomize success to simulate errors on cmds
    return random.random() >= prop_fail


action_lut = dict(
    echo30=echo30, # sample. Not used for production.
    network_move=tada.actions.network_move,
    submit=tada.actions.submit,
    )

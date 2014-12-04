'Constants to use for keys in redis'

aq = 'activeq'    # Active Queue. List of IDs. Pop and apply actions from this.
aqs = 'activeq_set'  # SET version of "activeq" so we can tell if an id
                     # is already on the active list
iq = 'inactiveq'   # List of IDs. Stash records that should not be popped here
iqs = 'inactiveq_set' # SET version of "inactiveq" so we can tell if an id
                      # is already on the inactive list

rids = 'record_ids' # Set of IDs used as keys to record hash.
                    # hmset(id,rec)

ecnt = 'errorcnt'      # errorcnt[id]=cnt; number of Action errors against ID
actionP = 'actionFlag' # on|off
readP = 'readFlag'     # on|off

dummy = 'dummy_aq' # List used to clear block of AQ when needed on change
                   # of actionFlag


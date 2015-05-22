'Constants to use for keys in redis'

aq='activeq'         # List of IDs. Pop and apply actions from this.
aqs='activeq_set'    # SET version of "activeq" so we can tell if an id
                     # is already on the active list

iq='inactiveq'       # List of IDs. Stash records that should not be popped here
iqs='inactiveq_set'  # SET version of "inactiveq" so we can tell if an id
                     # is already on the inactive list

rids='record_ids'    # Set of IDs used as keys to record hash.
#<id>=rec            # hmset(id,rec); id :: checksum

ecnt='errorcnt'      # errorcnt[id]=cnt; number of Action errors against ID
actionP='actionFlag' # on|off
readP='readFlag'     # on|off
dummy='dummy_aq'     # List used to clear block of AQ when needed on change
                     # of actionFlag

histb='history_begin'        # Earliest time for actions recorded in history
histp='history_pass_actions' # List of actions that passed since history_begin
histf='history_fail_actions' # List of actions that failed since history_begin

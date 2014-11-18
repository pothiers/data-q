"""Read and validate configuration file.  All three processes (pop
svc, push svc, cli) use this."""

import json


def get_config_lut(config):
    "Return dictionary indexed by queue name."
    return dict([[q['name'], q] for q in config['queues']])


REQUIRED_FIELDS = set('name,dq_host,dq_port,action_name'.split(','))


# OBSOLETE!!!
def get_config_by_name(queue_name, json_filename='/etc/dataq/dq.conf'):
    """Read multi-queue config from json_filename.  Validate its
contents. Insure queue_name is one of the named queues."""

    try:
        cfg = json.load(open(json_filename))
    except:
        raise Exception('ERROR: Could not read dataqueue config file "{}"'
                        .format(json_filename))

    
    for q in cfg['queues']:
        missing = REQUIRED_FIELDS - set(q.keys())
        if  len(missing) > 0:
            raise Exception('ERROR: Queue "{}" in {} is missing fields: {}'
                            .format(
                                q.get('name','UNKNOWN'),
                                json_filename,
                                missing))
        
    return get_config_lut(cfg)[queue_name]

def get_config(queue_names, json_filename='/etc/dataq/dq.conf'):
    """Read multi-queue config from json_filename.  Validate its
contents. Insure queue_names are all in the list of named queues."""

    try:
        cfg = json.load(open(json_filename))
    except:
        raise Exception('ERROR: Could not read dataqueue config file "{}"'
                        .format(json_filename))

    
    for q in cfg['queues']:
        missing = REQUIRED_FIELDS - set(q.keys())
        if  len(missing) > 0:
            raise Exception('ERROR: Queue "{}" in {} is missing fields: {}'
                            .format(
                                q.get('name','UNKNOWN'),
                                json_filename,
                                missing))

        
    lut = get_config_lut(cfg)
    missing = set(queue_names) - set(lut.keys())
    if len(missing) > 0:
        raise Exception(
            'ERROR: Config file "{}" does not contain named queues: {}'
            .format(json_filename, missing))
    return lut


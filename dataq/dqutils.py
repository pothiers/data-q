import os, os.path


def decode_dict(byte_dict):
    str_dict = dict()
    for k,v in list(byte_dict.items()):
        str_dict[k.decode()] = v.decode()
    return str_dict


def save_pid(progpath):
    base = os.path.basename(progpath)
    # Following is BAD!!! Platform dependent location of PID file.
    # init functions.daemon would not write pid. This was last resort.
    # What is the right way???
    pid = os.getpid()
    with open('/var/run/dataq/%s.pid'%(base),'w') as f:
        print(pid, file=f, flush=True)
    return pid
    

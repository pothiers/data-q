"""Interface to working with irods.  All of our calls to irods
provided functions should be in this file."""

import os.path
import subprocess
import logging

def irods_put(local_fname, irods_fname):
    'Put file to irods, creating parent directories if needed.'
    try:
        subprocess.check_output(['imkdir', '-p', os.path.dirname(irods_fname)])
        subprocess.check_output(['iput', '-f', local_fname, irods_fname])
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}'.format(ex))
        raise


def irods_get(local_fname, irods_fname):
    'Get file from irods, creating local parent directories if needed.'
    cmdargs1 = ['mkdir', '-p', os.path.dirname(local_fname)]
    cmdargs2 = ['iget', '-f', irods_fname, local_fname]
    try:
        subprocess.check_output(cmdargs1)
        subprocess.check_output(cmdargs2)
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}'.format(ex))
        raise

def irods_reg(fs_path, irods_path):
    """Register a file or a directory of files and subdirectory into
iRODS. The file must already exist on the server where the resource is
located. The full path must be supplied for both paths."""
    out = None
    cmdline = ['imkdir', '-p', os.path.dirname(irods_path)]
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise

    cmdline = ['ireg',  fs_path, irods_path]
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise
    return out
    

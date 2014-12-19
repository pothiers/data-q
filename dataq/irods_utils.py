"""Interface to working with irods.  All of our calls to irods
provided functions should be in this file."""

import os.path
import subprocess
import logging

# NB: The command must be installed in the server/bin/cmd directory
#     of the irods server
# e.g.
#     sudo cp /usr/bin/file_type /var/lib/irods/iRODS/server/bin/cmd/
def irods_file_type(irods_fname):
    cmdline = ['iexecmd', '-P', irods_fname, 'file_type']
    out = 'NONE'
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise
    return out.decode('utf-8')[:-1]


# NB: The command must be installed in the server/bin/cmd directory
#     of the irods server
def irods_prep_fits_for_ingest(irods_filepath):
    """Given an irods absolute path that points to a FITS file, add fields
to its header and rename it so it is (probably) suitable for Archive
Ingest. The name returned is an irods absolute ipath to a hdr file
that can be passed as the hdrUri argument to the NSAserver."""

    hdr_ifname = None
    cmdline = ['iexecmd', '-P', irods_filepath,
               'prep_fits_for_ingest {}'.format(irods_filepath) ]
    try:
        hdr_ifname = subprocess.check_output(cmdline).decode('utf-8')[:-1]
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise

    # e.g. /noao-tuc-z1/mtn/20140930/kp4m/2014B-0108/k4m_141001_121310_ori.hdr
    return hdr_ifname

def irods_mv(src_ipath, dst_ipath):
    'Move/rename irods file'
    # imv /tempZone/mountain_mirror/vagrant/13/foo.nhs_2014_n14_299403.fits /tempZone/mountain_mirror/vagrant/13/nhs_2014_n14_299403.fits

    cmdline = ['imv', src_ipath, dst_ipath]
    out = None
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise
    return out

def irods_put(local_fname, irods_fname):
    'Put file to irods, creating parent directories if needed.'
    #os.chmod(local_fname, 0o664)
    try:
        subprocess.check_output(['imkdir', '-p',
                                 os.path.dirname(irods_fname)])
        subprocess.check_output(['iput', '-f', '-K',
                                 local_fname, irods_fname])
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}'.format(ex))
        raise


def irods_get(local_fname, irods_fname):
    'Get file from irods, creating local parent directories if needed.'
    cmdargs1 = ['mkdir', '-p', os.path.dirname(local_fname)]
    cmdargs2 = ['iget', '-f', '-K', irods_fname, local_fname]
    try:
        subprocess.check_output(cmdargs1)
        subprocess.check_output(cmdargs2)
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}'.format(ex))
        raise

def irods_unreg(irods_path):
    "unregister the file or collection"
    out = None
    cmdline = ['irm', '-U', irods_path]
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise
    return out

def irods_reg(fs_path, irods_path):
    """Register a file or a directory of files and subdirectory into
iRODS. The file must already exist on the server where the resource is
located. The full path must be supplied for both paths."""
    logging.warning('Use of iRODS "ireg" command SHOULD BE AVOIDED!')
    out = None
    cmdline = ['imkdir', '-p', os.path.dirname(irods_path)]
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise

    os.chmod(fs_path, 0o664)
    cmdline = ['ireg',  '-K', fs_path, irods_path]
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise
    return out
    

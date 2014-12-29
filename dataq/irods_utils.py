"""Interface to working with irods.  All of our calls to irods
provided functions should be in this file."""

import os.path
import subprocess
import logging

# imeta add -d /tempZone/mirror/vagrant/32/no_PROPID.fits x 1
# imeta set -d /tempZone/mirror/vagrant/32/no_PROPID.fits y 2
# imeta ls  -d /tempZone/mirror/vagrant/32/no_PROPID.fits
#
# isysmeta mod /tempZone/mirror/vagrant/32/no_PROPID.fits datatype 'FITS image'
# isysmeta ls -l /tempZone/mirror/vagrant/32/no_PROPID.fits 

# Get the pysical location
# iquest "%s" "select DATA_PATH where DATA_NAME = 'k4n_20141114_122626_oru.fits'"

def irods_get_physical(ipath):

    #!!! Open access to vault
    out = 'NONE'
    cmdline = ['iexecmd', 'open_vault.sh']
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise

    sel = ("select DATA_PATH where COLL_NAME = '{}' and DATA_NAME = '{}'"
           .format(os.path.dirname(ipath), os.path.basename(ipath)))
    cmdline = ['iquest', '"%s"', sel]
    out = 'NONE'
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise
    return out.decode('utf-8').strip('\n \"')
    
# NB: The command must be installed in the server/bin/cmd directory
#     of the irods server
# e.g.
#     sudo cp /usr/bin/file_type /var/lib/irods/iRODS/server/bin/cmd/
def irods_file_type(irods_fname):
    # lut[file_type.py_key] = irods_data_type (list all using "isysmeta ldt")
    lut = dict(FITS = 'FITS image',
               JPEG = 'jpeg image',
               )
    cmdline = ['iexecmd', '-P', irods_fname, 'file_type {}'.format(irods_fname)]
    out = 'NONE'
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise
    typeStr = out.decode('utf-8')[:-1]

    # Set datatype in metadata
    dt = lut.get(typeStr,'generic')
    cmdline = ['isysmeta', 'mod', irods_fname, 'datatype', dt]
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise

    return typeStr

# NB: The command "prep_fits_for_ingest" must be installed in the
#     server/bin/cmd directory of the irods server
def OLD_irods_prep_fits_for_ingest(irods_filepath, mirror_idir, archive_idir):
    """Given an irods absolute path that points to a FITS file, add fields
to its header and rename it so it is (probably) suitable for Archive
Ingest. The name returned is an irods absolute ipath to a hdr file
that can be passed as the hdrUri argument to the NSAserver."""

    hdr_ifname = None
    #!irods_set_meta(irods_filepath, 'prep', 'False')
    cmdline = ['iexecmd', '-P', irods_filepath,
               'prep_fits_for_ingest {} {} {}'
               .format(irods_filepath, mirror_idir, archive_idir) ]
    try:
        hdr_ifname = subprocess.check_output(cmdline).decode('utf-8')[:-1]
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise

    # set "prep" attribute of fits_ifile
    #!irods_set_meta(irods_filepath, 'prep', 'True')

    # e.g. /noao-tuc-z1/mtn/20140930/kp4m/2014B-0108/k4m_141001_121310_ori.hdr
    return hdr_ifname


def irods_set_meta(ifname, att_name, att_value):
    cmdline = ['imeta', 'set', '-d', ifname, 'prep', 'True']
    out = 'NONE'
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output))
        raise


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

def irods_mv_tree(src_ipath, dst_ipath):
    'Move/rename irods file including parent directories' 
    # mv /tempZone/mirror/vagrant/13/foo.fits /tempZone/archive/vagrant/13/foo.fits
    try:
        subprocess.check_output(['imkdir', '-p',
                                 os.path.dirname(dst_ipath)])
        subprocess.check_output(['imv', src_ipath, dst_ipath])
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}'.format(ex))
        raise



#!!!
def bridge_copy(src_ipath, mirror_idir, archive_idir): # dst_ipath, qname, qcfg
    logging.error('EXECUTING STUB!!!: bridge_copy({}, {}, {})'
                  .format(src_ipath, mirror_idir, archive_idir))
    dst_ipath = src_ipath.replace(mirror_idir, archive_idir)
    irods_mv_tree(src_ipath, dst_ipath) #need to cross over irods INSTANCES!!!
    return dst_ipath



def irods_mv_dir(src_idir, dst_idir):
    """Move irods directory (collection) from one place to another,
creating desting parent directories if needed."""
    try:
        subprocess.check_output(['imkdir', '-p', dst_idir])
        subprocess.check_output(['imv', src_idir, dst_idir])
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}'.format(ex))
        raise

def irods_put(local_fname, irods_fname ):
    'Put file to irods, creating parent directories if needed.'
    #os.chmod(local_fname, 0o664)
    try:
        subprocess.check_output(['imkdir', '-p',
                                 os.path.dirname(irods_fname)])
        subprocess.check_output(['iput', '-f', '-K', 
                                 local_fname, irods_fname])
        top_ipath = '/' + irods_fname.split('/')[1]
        subprocess.check_output(['ichmod', '-r', 'own', 'public', top_ipath])
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
    logging.warning('EXECUTING: "irm -U {}"; Remove need!!!'.format(irods_path))
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
    

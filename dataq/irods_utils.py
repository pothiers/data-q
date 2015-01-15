"""Interface to working with irods.  All of our calls to irods
provided functions should be in this file."""

import os
import os.path
import subprocess
import logging
import tempfile

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
                      .format(ex, cmd, ex.output.decode('utf-8')))
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
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise
    return out.decode('utf-8').strip('\n \"')

def irods_debug():
    """For diagnostics only."""
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        cmdline = ['ienv']
        try:
            out = subprocess.check_output(cmdline)
        except subprocess.CalledProcessError as ex:
            cmd = ' '.join(cmdline)
            logging.error('Execution failed: {}; {} => {}'
                          .format(ex, cmd, ex.output.decode('utf-8')))
            raise
        logging.debug('ienv={}'.format(out.decode('utf-8')))
    

# NB: The command must be installed in the server/bin/cmd directory
#     of the irods server
# e.g.
#     sudo cp /usr/bin/file_type /var/lib/irods/iRODS/server/bin/cmd/
def irods_file_type(irods_fname):
    logging.debug('irods_file_type({})'.format(irods_fname))

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
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise
    typeStr = out.decode('utf-8')[:-1]

    # Set datatype in metadata. Not used. Retained for future use.
    #! dt = lut.get(typeStr,'generic')
    #! cmdline = ['isysmeta', 'mod', irods_fname, 'datatype', dt]
    #! try:
    #!     out = subprocess.check_output(cmdline)
    #! except subprocess.CalledProcessError as ex:
    #!     cmd = ' '.join(cmdline)
    #!     logging.error('Execution failed: {}; {} => {}'
    #!                   .format(ex, cmd, ex.output.decode('utf-8')))
    #!     raise

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
                      .format(ex, cmd, ex.output.decode('utf-8')))
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
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise


def irods_mv(src_ipath, dst_ipath):
    'Move/rename irods file' 
    logging.debug('irods_mv({}, {})'.format(src_ipath, dst_ipath))
    # imv /tempZone/mountain_mirror/vagrant/13/foo.nhs_2014_n14_299403.fits /tempZone/mountain_mirror/vagrant/13/nhs_2014_n14_299403.fits

    cmdline = ['imv', src_ipath, dst_ipath]
    out = None
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise
    return out

def irods_mv_tree(src_ipath, dst_ipath):
    'Move/rename irods file including parent directories' 
    logging.debug('irods_mv_tree({}, {})'.format(src_ipath, dst_ipath))
    # mv /tempZone/mirror/vagrant/13/foo.fits /tempZone/archive/vagrant/13/foo.fits
    try:
        subprocess.check_output(['imkdir', '-p',
                                 os.path.dirname(dst_ipath)])
        subprocess.check_output(['imv', src_ipath, dst_ipath])
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}; {}'.format(ex, ex.output.decode('utf-8')))
        raise

 

# light-weight but dangerous.
# Register file in irods331  to physical file under irods403.
def fast_bridge_copy(src_ipath, dst_ipath, remove_orig=False):
    logging.error(':Start EXECUTING TEMPORARY HACK!!!: bridge_copy({}, {})'
                  .format(src_ipath, dst_ipath))
    local_file = irods_get_physical(src_ipath)
    irods_reg_331(local_file, dst_ipath)

    logging.error(':Done EXECUTING TEMPORARY HACK!!!')
    return dst_ipath
    
# This does expensive iget, iput combination!!!
# When Archive moves up to irods 4, we can dispense with this nonsense!
def bridge_copy(src_ipath, dst_ipath, remove_orig=False): 
    logging.error(':Start EXECUTING OLD TEMPORARY HACK!!!: bridge_copy({}, {})'
                  .format(src_ipath, dst_ipath))

    (fd, temp_fname) = tempfile.mkstemp()
    os.close(fd)
    cmdargs1 = ['iget', '-f', src_ipath, temp_fname]
    try:
        subprocess.check_output(cmdargs1)
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}; {}'
                      .format(ex,
                              ex.output.decode('utf-8')))
        raise
    logging.debug('Successful iget {} into {}'.format(src_ipath, temp_fname))
    irods_put331(temp_fname, dst_ipath)
    logging.debug('Successful put-331 {} into {}'.format(temp_fname, dst_ipath))
    os.unlink(temp_fname)

    # Only happens if iget and iput succeed
    if remove_orig:
        cmdargs2 = ['irm', '-f', '-U', src_ipath]
        try:
            subprocess.check_output(cmdargs2)
        except subprocess.CalledProcessError as ex:
            logging.error('Execution failed: {}; {}'
                          .format(ex, ex.output.decode('utf-8')))
            raise
        
    logging.error(':Done EXECUTING TEMPORARY HACK!!!')
    return dst_ipath



def irods_mv_dir(src_idir, dst_idir):
    """Move irods directory (collection) from one place to another,
creating desting parent directories if needed."""
    try:
        subprocess.check_output(['imkdir', '-p', dst_idir])
        subprocess.check_output(['imv', src_idir, dst_idir])
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}; {}'
                      .format(ex, ex.output.decode('utf-8')))
        raise

def irods_put(local_fname, irods_fname):
    'Put file to irods, creating parent directories if needed.'
    logging.debug('irods_put({}, {})'.format(local_fname, irods_fname))

    #os.chmod(local_fname, 0o664)
    try:
        subprocess.check_output(['imkdir', '-p',  os.path.dirname(irods_fname)])
        subprocess.check_output(['iput', '-f', '-K', local_fname, irods_fname])
        #! top_ipath = '/' + irods_fname.split('/')[1]
        #! subprocess.check_output(['ichmod', '-r', 'own', 'public', top_ipath])
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}; {}'
                      .format(ex, ex.output.decode('utf-8')))
        raise

# For iRODS 3.3.1 server
archiveIrods = dict(
    irodsEnvFile='/sandbox/archIrodsEnv',
    irodsAuthFileName='/sandbox/archIrodsAuth'
    )

# For iRODS 4.0.3 servers
tadaIrods = dict(
    irodsEnvFile='/sandbox/tadaIrodsEnv',
    irodsAuthFileName='/sandbox/tadaIrodsAuth'
    )

def irods_put331(local_fname, irods_fname):
    logging.debug('irods_put333({}, {})'.format(local_fname, irods_fname))
    
    try:
        subprocess.check_output(
            ['/sandbox/tada/scripts/put_other_host.sh {} {}'
             .format(local_fname, irods_fname)],
            stderr=subprocess.STDOUT,
            shell=True)
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}; {}'
                      .format(ex, ex.output.decode('utf-8')))
        raise
    
def BROKENirods_put331(local_fname, irods_fname):
    '''Put file to irods, creating parent directories if needed.
This is HACK to support legacy iRODS 3.3.1 server.'''
    env1 = os.environ.copy()
    env1.update(tadaIrods)
    env2 = os.environ.copy()
    env2.update(archiveIrods)
    env2['PATH'] = ('/sandbox/irods3.3.1/iRODS/clients/icommands/bin:'
                    +env2['PATH'])
    logging.debug('irods_put331({}, {}); env2="{}"'
                  .format(local_fname, irods_fname, env2))

    try:
        logging.debug('  rm ~/.irods/.irodsA')
        os.remove(os.path.join(env2['HOME'],'.irods','.irodsA'))
        logging.debug('  iinit (archiveEnv)')
        subprocess.check_output(['iinit', '-V', 'cacheMonet'],
                                stderr=subprocess.STDOUT,
                                env=env2,
                                timeout=2,
                                shell=True)

        logging.debug('  imkdir')
        subprocess.check_output(['imkdir', '-p',  os.path.dirname(irods_fname)],
                                env=env2)
        logging.debug('  iput')
        subprocess.check_output(['iput', '-f', '-K', local_fname, irods_fname],
                                env=env2)
        logging.debug('  iinit (tadaEnv)')
        subprocess.check_output(['iinit', 'temppasswd'], env=env1)
        #! top_ipath = '/' + irods_fname.split('/')[1]
        #! subprocess.check_output(['ichmod', '-r', 'own', 'public', top_ipath])
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}; {}'
                      .format(ex, ex.output.decode('utf-8')))
        raise



def irods_get(local_fname, irods_fname, remove_irods=False):
    'Get file from irods, creating local parent directories if needed.'
    os.makedirs(os.path.dirname(local_fname), exist_ok=True)
    cmdargs1 = ['iget', '-f', '-K', irods_fname, local_fname]
    try:
        subprocess.check_output(cmdargs1)
    except subprocess.CalledProcessError as ex:
        logging.error('Execution failed: {}; {}'
                      .format(ex, ex.output.decode('utf-8')))
        raise

    if remove_irods:
        cmdargs2 = ['irm', '-f', '-U', irods_fname]
        try:
            subprocess.check_output(cmdargs2)
        except subprocess.CalledProcessError as ex:
            logging.error('Execution failed: {}; {}'
                          .format(ex, ex.output.decode('utf-8')))
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
                      .format(ex, cmd, ex.output.decode('utf-8')))
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
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise

    os.chmod(fs_path, 0o664)
    cmdline = ['ireg',  '-K', fs_path, irods_path]
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise
    return out

def irods_reg_331(fs_path, irods_path):
    "Like irods_reg except registers on irods 331 server"
    logging.warning('Use of iRODS "ireg" (331) command SHOULD BE AVOIDED!')
    logging.debug('irods_reg_331({}, {})'.format(fs_path, irods_path))

    env1 = os.environ.copy()
    env1.update(tadaIrods)
    env2 = os.environ.copy()
    env2.update(archiveIrods)
    
    out = None
    cmdline = ['imkdir', '-p', os.path.dirname(irods_path)]
    try:
        out = subprocess.check_output(cmdline, env=env2, shell=True)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise
    finally:
        subprocess.check_output(['iinit', 'temppasswd'], env=env1)

    os.chmod(fs_path, 0o664)
    cmdline = ['ireg',  '-K', fs_path, irods_path]
    try:
        out = subprocess.check_output(cmdline, env=env2, shell=True)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise
    finally:
        subprocess.check_output(['iinit', 'temppasswd'], env=env1)
    return out
    
    
def get_irods_cksum(irods_path):
    cmdline = ['ichksum', irods_path]
    try:
        out = subprocess.check_output(cmdline)
    except subprocess.CalledProcessError as ex:
        cmd = ' '.join(cmdline)
        logging.error('Execution failed: {}; {} => {}'
                      .format(ex, cmd, ex.output.decode('utf-8')))
        raise
    return(out.split()[1].decode('utf-8'))
    

'''
iadmin mkzone noao-tuc-z1 remote
iadmin mkuser cache#noao-tuc-z1 rodsadmin
iadmin moduser cache#noao-tuc-z1 password money4nuthin

irodsEnvFile=~/.irods/archIrodsEnv iinit money4nuthin


iinit temppasswd
ichmod -r own public /noao-tuc-z1

irodsEnvFile='/sandbox/archIrodsEnv' iinit money4nuthin
irodsEnvFile='/sandbox/archIrodsEnv' iput -f -K ~/foo.txt /noao-tuc-z1/foo.txt
irodsEnvFile='/sandbox/archIrodsEnv' ils /noao-tuc-z1


'''

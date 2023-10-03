# local.py

import sys
import os
import shutil

__version__  = '0.0.2'

def install_dir(use=True, rem=False):
    """
    Return the name of the local installation directory.
    The name is specific to the LSST conda env name.
    The directory is created if it does not already exist.
    If use is true, the directory is added to sys.path if
    if is not already present.
    If rem is true, the installation directory is removed
    and the created empty.
    """
    basdir = './install'
    conda_name = os.getenv('LSST_CONDA_ENV_NAME')
    if conda_name is None or len(conda_name)==0:
        conda_name = 'noconda'
    insdir = os.path.join(basdir, conda_name)
    if rem and os.path.exists(insdir):
        shutil.rmtree(insdir)
    if not os.path.exists(insdir):
        os.makedirs(insdir)
        fin = os.path.join(os.path.dirname(__file__), 'setup-install.py')
        fout = os.path.join(basdir, 'setup.py')
        com = shutil.copyfile(fin, fout)
    #%pip install -t {insdir} {opts} {pkgpath}
    if use and insdir not in sys.path:
        sys.path.insert(0, insdir)
    return insdir

def install(pkgname, pkgdir='', opts='', rem=False):
    """
    Use pip to install a package in the local installation directory.
    """
    if len(pkgdir):
        pkgpath = os.path.join(pkgdir, pkgname)
    else:
        pkgpath = pkgname
    insdir = install_dir(rem=rem)
    print(f"Installing {pkgpath} at {insdir}")
    #opt = '--upgrade'
    #com = f"PIP_PREFIX= {sys.executable} -m pip install --no-deps -t {insdir} {opts} {pkgpath}"
    com = f"PIP_PREFIX= {sys.executable} -m pip install -t {insdir} {opts} {pkgpath}"
    os.system(com)
    # Better for jupyter?
    #%pip install -t {insdir} {opts} {pkgpath}

# local.py

import sys
import os

__version__  = '0.0.1'

def use_local_install_dir():
    """
    Return the name of the local installation directory.
    The name is specific to the LSST conda env name.
    The directory is created if it does not already exist.
    If use is true, the directory is added to sys.path if
    if is not already present.
    """
    conda_name = os.getenv('LSST_CONDA_ENV_NAME')
    if conda_name is None or len(conda_name)==0:
        conda_name = 'noconda'
    insdir = os.path.join('./install', conda_name)
    if insdir not in sys.path:
        sys.path.insert(0, insdir)

use_local_install_dir()

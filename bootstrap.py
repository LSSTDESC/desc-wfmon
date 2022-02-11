# bootstrap.py
#
# David Adams
# February 2022
#
# This script can be used to install this package (desc-wfmon)
# into a local install area (./install) `for the first time.
# the syntax is:
# >>> pkgdir = 'PKGDIR'
# >>> exec(open(pkgdir + '/bootstrap.py').read())
# where PKGDIR is the directory hroplding this package.

import sys
import os
import shutil
import importlib

# Make sure the installation directory is on sys.path.
insdir = 'install/'+os.getenv('LSST_CONDA_ENV_NAME')
if insdir not in sys.path:
    print(f"bootstrap: Adding {insdir} to sys.path")
    sys.path.insert(0, insdir)

# Check if desc.local is available.
try:
    import desc.local
    print(f"bootstrap: Found desc.local version {desc.local.__version__}. No action taken.")
    print('bootstrap: To update or reinstall, please use desc.local.install.')
except ImportError as e:
    print(f"bootstrap: Import failed: {e}")
    print(f"bootstrap: Installing desc.wfmon in {insdir}.")
    tmppkg = 'tmplocal'
    os.mkdir('tmplocal')
    try:
        shutil.copyfile(pkgdir+'/desc/local/local.py', 'tmplocal/local.py')
        fout = open('tmplocal/__init__.py', 'w')
        fout.write("__version__ = 'bootstrap'")
        fout.close()
        import tmplocal.local
        tmplocal.local.install('desc-wfmon', os.path.dirname(pkgdir))
    finally:
        shutil.rmtree(tmppkg)

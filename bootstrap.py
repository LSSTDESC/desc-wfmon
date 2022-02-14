# bootstrap.py
#
# David Adams
# February 2022
#
# This script can be used to install this package (desc-wfmon)
# into the local install area (./install/$LSST_CONDA_ENV_NAME)
#for the first time.  the syntax is:
# >>> exec(open('DIR/bootstrap.py').read())
# or from a notebook,
# %run DIR/bootstrap.py

import sys
import os
import shutil
import importlib

doInstall = True
if doInstall:
    print(f"bootstrap: Installing descr-wfmon.")
    tmppkg = 'tmplocal'
    os.system('rm -rf ./tmplocal')
    os.mkdir('tmplocal')
    pkgdir = os.path.dirname(__file__)
    try:
        for fnam in ['local.py', 'setup-install.py']:
            fin = os.path.join(pkgdir, 'desc/local', fnam)
            fout = os.path.join('tmplocal', fnam)
            shutil.copyfile(fin, fout)
        fout = open('tmplocal/__init__.py', 'w')
        fout.write("__version__ = 'bootstrap'")
        fout.close()
        import tmplocal.local
        tmplocal.local.install('desc-wfmon', os.path.dirname(pkgdir), rem=True)
        import desc.local
        insdir = desc.local.install_dir()
        print(f"bootstrap: Package desc-wfmon is installed at {insdir}")
    except e:
        print(f"bootstrap: ERROR: Installation failed: {e}.")
    finally:
        print('Done')
        #shutil.rmtree(tmppkg)

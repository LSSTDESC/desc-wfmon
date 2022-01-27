#!/usr/bin/env python

from distutils.core import setup

setup(name='desc-wfmon',
      version='0.01',
      description='DESC workflow minitor',
      author='David Adams',
      author_email='dladams@bnl.gov',
      url='https://github.com/LSSTDESC/desc-wfmon',
      packages=['desc/wfmon', 'desc/sysmon'],
     )

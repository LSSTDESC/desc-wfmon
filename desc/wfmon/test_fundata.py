# test_fundata.py
#
# David Adams
# March 2022

"""
Testing for FunData.
"""

import unittest
from desc.wfmon import FunctionData

class TestFunctionData(unittest.TestCase):
    """
    Class to test FunctionData. Run with
      > nosetests -v fundata.py
    or
      > nosetests -v --pdb fundata.py  # Drops into debugger on error or exception.
    """

    def test_build(self, dbg=0):
        myname = 'TestFunctionData:init'
        prefix = '/global/homes/d/dladams/desc/rundirs/ptest05/'
        fdat = FunctionData(prefix, dbg=dbg)
        print(fdat.data)
        self.assertTrue(len(fdat.data))
        print(f"{myname}: Done.")

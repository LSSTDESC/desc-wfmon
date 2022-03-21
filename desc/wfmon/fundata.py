# fundata.py
#
# David Adams
# March 2022

"""
Read parsl function_data into a DataFrame.
"""

import os
import glob
import pandas

class FunctionData:

    def __init__(self, prefix="./", pattern='runinfo/???/function_data/*/log', dbg=0):
        """
        Function data is extracted from files matching
        <prefix><pattern>/function_data/<taskid>/log
        Result is in dataframe self.table.
        """
        myname = 'FunctionData:ctor'
        self.data = None
        patpat = prefix + pattern
        fnams = glob.glob(patpat)
        fnams.sort()
        if dbg: print(f"{myname}: Search pattern is {patpat}")
        if dbg: print(f"{myname}: File count is {len(fnams)}")
        nent = 0
        for fnam in fnams:
            if dbg >= 2: print(f"{myname}: Reading {fnam}")
            errmsg = ''
            nline = 0
            fin = open(fnam)
            stid = os.path.basename(os.path.dirname(fnam))
            while len(stid) > 1 and stid[0] == '0': stid = stid[1:]
            try:
                taskid = int(stid)
            except:
                errmsg = f"Unable to extract task from path {fnam}"
            if len(errmsg) == 0:
                mymap = {'taskid':taskid}
                if dbg >= 4: print(f"{myname}:     taskid: {taskid}")
                for line in fin:
                    wrds = line.rstrip().split()
                    if len(wrds) == 2:
                        tim = float(wrds[0])
                        nam = wrds[1].lower()
                        mymap[nam] = tim
                        if dbg >= 4: print(f"{myname}:     {nam} : {tim}")
                        nline += 1
                    else:
                        msg = f"Unexpected line {nline} in file {fnam}: line.rstrip()"
            if len(errmsg):
                print(f"{myname}: ERROR: {errmsg}")
            else:
                df = pandas.DataFrame(mymap, index=[nent])
                if self.data is None:
                    self.data = pandas.DataFrame(columns=mymap.keys(), index=range(len(fnams)))
                self.data.loc[nent] = mymap
                nent += 1
            if dbg >= 3: print(f"{myname}:   Line read count: {nline}")


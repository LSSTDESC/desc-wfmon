# perfstat.py
#
# David Adams
# March 2022

"""
Fetch perf stat data from a log file.
"""

import os
import glob
import collections
import pandas

class PerfStatLogReader:

    def __init__(self, dbg=0):
        """
        Holds perf stat data from one or more log files.
        Dictionary is value lists indexed by field name.
        """
        self.flglin = 'Performance counter stats for'
        self.dbg = dbg
        self.nfile = 0
        self.nval = 0
        self.nmissing = 0
        self.dict = collections.OrderedDict()
        self._table = pandas.DataFrame()

    def table(self):
        """
        Return results as dataframe.
        """
        if self.nval == 0: return None
        if self.nval > len(self._table):
            self._table = pandas.DataFrame(self.dict)
        return self._table

    def read(self, fnam, indata={}, dbg=0):
        """
        Read data from a file.
        The table is extended using the supplied map (indata) and the
        per stat data found in the file.
        """
        myname = "PerfStatLogReader:ctor"
        # build a log label from the input data.
        slab = str(self.nval)
        for key in indata:
            slab = slab + '-' + str(indata[key])
        self.nfile = self.nfile + 1
        dbg = self.dbg
        data = indata
        with open(fnam) as fin:
            if dbg >= 1:
                print(f"{myname} {slab}: Opened {fnam}")
            inperf = False
            done = False
            nlin = 0
            nprf = 0
            while True:
                line = fin.readline()
                if len(line) == 0: break
                line = line.strip()
                if dbg >= 4:
                    print(f"{myname} {slab}: {'*' if inperf else ' '}  {nlin:5d}: {line}")
                nlin = nlin + 1
                if inperf:
                    sublines = line.split('#')
                    if len(sublines) < 1:
                        if len(sublines) == 1:
                            print(f"{myname} {slab}: Skipping unexpected line: {line}")
                        continue
                    ldat = sublines[0]
                    words = ldat.split()
                    if len(words) > 1:
                        sval = words[0].replace(',', '')
                        if sval[0] == '<':
                            pass       # Missing value, e.g. "<not supported>"
                        elif sval in ['real', 'user', 'sys']:
                            done = True
                        else:
                            sunit = ''
                            inam = 1
                            if len(words) > 2:
                                inam = 2
                                sunit = words[1]
                                val = float(sval)
                            else:
                                val = int(sval)
                            snam = words[inam]
                            inam = inam + 1
                            while inam < len(words):
                                snam = snam + '-' + words[inam]
                                inam = inam + 1
                            nprf = nprf + 1
                            if dbg > 1:
                                print(f"{myname} {slab}: {snam} = {val}")
                            # Add the name-value to the dictionary.
                            if snam in data:
                                print(f"{myname} {slab}: WARNING: Overwriting duplicate value for {snam}")
                            data[snam] = val
                elif self.flglin in line:
                     inperf = done == False
        if dbg >= 1:
            print(f"{myname} {slab}: Read {nlin} lines and found {nprf} metrics ==> {len(data)} columns")
        if nprf > 0:
            # Copy data to self.dict
            for snam in data:
                val = data[snam]
                if snam not in self.dict:
                    self.dict[snam] = [None] * self.nval
                self.dict[snam].append(val)
            # Pad any missing values.
            for snam in self.dict:
                vals = self.dict[snam]
                nadd = 0
                nval = self.nval + 1
                while len(vals) < nval:
                    nadd = nadd + 1
                    vals.append(None)
                if nadd:
                    print(f"{myname} {slab}: Padded {nadd} values metric {snam}.")
                assert(len(vals) == nval)
            self.nval = nval
        else:
            print(f"{myname} {slab}: No metrics found.")
            self.nmissing = self.nmissing + 1

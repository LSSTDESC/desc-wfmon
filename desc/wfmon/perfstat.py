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
import desc.wfmon

class PerfStatLogReader:

    def __init__(self, dbg=0):
        """
        Holds perf stat data from one or more log files.
        Dictionary is value lists indexed by field name.
        """
        self.flglin = 'Performance counter stats for'
        self.dbg = dbg
        self.nfile = 0         # # of attempted log file reads
        self.nkeep = 0         # # successful lof file reads
        self.nmissing = 0      # # # file read missing data
        self.nskip = 0         # # files not read or missing data
        self.nval = 0          # # entries in the table
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

    def read_log(self, fnam, indata={}, dbg=0):
        """
        Read data from a file.
        The table is extended using the supplied map (indata) and the
        perf stat data found in the file.
        Arguments:
          fnam - Input log file.
          indata - map of index data to add to the table
          dbg - 0 for silent
        Returns 0 if successful
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
            self.nkeep = self.nkeep + 1
            return 0
        else:
            print(f"{myname} {slab}: No metrics found.")
            self.nmissing = self.nmissing + 1
            self.nskip = self.nskip + 1
            return 1


    def read_mondb_logs(self, dbr, dbg=0):
        """
        Read data from logs referenced by a MondDbReader.
        Each succesfully read log adds an entry to the perfstat table here.
        The table is extended using the supplied map (indata) and the
        per stat data found in the file.
        Arguments:
          dbr - MonDbReader
          dbg - 0=silent
        Return is a map:
          nkeep - # files read succesfully
          nskip - # files skipped
        """
        myname = 'PerfStatLogReader::read_mondb_logs'
        if 'task' not in dbr.fixed:
            if dbg:
                print(f"{myname}: Fixing input task table.")
            dbr.fix_tasks()
        if 'times' not in dbr.fixed:
            if dbg:
                print(f"{myname}: Fixing input times.")
            dbr.fix_times()
        tsk = dbr.table('task')
        ttr = dbr.table('try')
        nkeep = 0
        nskip = 0
        for tid in range(len(tsk)):
            indict = collections.OrderedDict()
            for nam in ['run_idx', 'task_idx', 'task_id']: indict[nam] = tsk[nam][tid]
            sqry = f"run_idx=={indict['run_idx']} and task_id=={indict['task_id']}"
            # Find the matching row(s) in the try table and extract the run start and stop times.
            ttrm = ttr.query(sqry)
            nttrm = len(ttrm)
            if len(ttrm) != 1:
                print(f"{myname}: WARNING: Try count is {nttrm} for query '{sqry}' ")
            indict['tstart'] = ttrm.task_try_time_running.iloc[0]
            indict['tstop'] = ttrm.task_try_time_returned.iloc[0]
            fnam = dbr.task_logs[tid]
            try:
                if not os.path.exists(fnam):
                    if dbg >= 1:
                        print(f"  File not found {fnam}")
                    nskip = nskip + 1
                else:
                    if self.read_log(fnam, indict, max(dbg-1, 0)):
                        if dbg >= 2:
                            print(f"  Error reading {fnam}")
                        nskip = nskip + 1
                        self.nskip = self.nskip + 1
                    else:
                        if dbg >= 2:
                            print(f"  Read {fnam}")
                        nkeep = nkeep + 1
            except:
                if fnam is not None:
                    print(f"{myname}: Skipping  task {tid} with invalid log path: {fnam}")
                nskip = nskip + 1
        if dbg >= 1:
            print(f"{myname}: nskip = {nskip}")
            print(f"{myname}: nkeep = {nkeep}")
            print(f"{myname}: nfile = {self.nfile}")
            print(f"{myname}:  nval = {self.nval}")
            print(f"{myname}: nmiss = {self.nmissing}")
            print(f"{myname}: Perf state names: {self.dict.keys()}")
        n_cycles = 'cycles:u'
        n_clock = 'task-clock:u'
        tab = self.table()
        tab['speed'] = tab[n_cycles]/tab[n_clock]
        return {"nkeep":nkeep, "nskip":nskip}

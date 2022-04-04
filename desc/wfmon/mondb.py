# mondb.py
#
# Class to access the parsl monitoring database.
#
# David Adams
# December 2021

import pandas
import numpy
import sys
import datetime
from IPython.display import display

class MonDbReader:
    filename = "monitoring.db"

    def __init__(self, filename ='./monitoring.db', fix=True, dodelta=False, dbg=0):
        """
        MuonDbReader provides access to a parsl monitoring database which has information
        about workflows and their tasks including start and stop times and process information.
        Arguments:
          filename: Name of the MySQL file holding the minotoring data.
               fix: if True, the tables are "fixed".
               dodelta: if True, deltas are evaluated when the procsum table is constructed.
               dbg: Debugging level:
                      0 - Quiet.
                      1 - Single line indicating methods that are run plus warning messages.
                      2 - Few lines summarizing results of each method.
                      3, 4, ... - Increasing level of detail.
        """
        self.filename = None            # Name of the source file
        self.dbg = dbg                  # Debugging level (see above)
        self.workflow_names = []        # List of workflow (run) names.
        self.workflow_time_ranges = []  # List of time range in seconds for each workflow
        self.run_ids = []               # List of run IDs in the original task table.
        self.task_names = []            # List of task names (index is the task index).
        self.task_name_counts = []      # List of the number tasks for each task index.
        self.taskIndexFromName = {}     # Task index for each task name.
        self.task_index = []            # Task index for each run index and task ID.
        self.task_logs = []             # Log file for task and try
        self.fixed = []                 # List of properties that have been fixed: workflows, times, ...
        self.remove_counts = {}         # Number of rows removed from each table.
        self.t0 = 0                     # Time offset (sec) for all fixed times.
        self.monitoring_interval = None
        self.taskprocs = [[], []]       # Dictionary of nodeleta,delta taskproc tables for each run index.
        self._taskcount_delt = 0        # Time spacing for the task count tables.
        self._taskcounts = []           # Run-indexed array of task-indexed arrays of time:state dfs.
        self._con = None
        self._tables = {}
        self.nwarnNoOffset = 0

        """Construct from the path to the monitoring DB file [monitoring.db]."""
        if len(filename): self.filename = filename
        self._connect()
        if fix: self.fix(dodelta)

    def __getitem__(self, tnam):
        return self.table(tnam, 0)

    def __len__(self):
        """len(obj) returns the number of tables."""
        return len(self._tables)

    def _connect(self, reconnect = False):
        """Connect to DB and read tables (if needed) and return connection string."""
        myname = self.__class__.__name__ + "::_connect"
        if self._con is None or reconnect:
           import sqlite3
           self._tables = {}
           try:
               self._con = sqlite3.connect(self.filename)
           except:
               print(f"""{myname}: ERROR: Unable to open file {self.filename}""")
               print(sys.exc_info()[1])
               return None
           tnams = list(pandas.read_sql_query("select name from sqlite_master where type='table'", self._con)['name'])
           for tnam in tnams:
               self._tables[tnam] = pandas.read_sql_query(f"""select * from {tnam}""", self._con)
               self.remove_counts[tnam] = 0
        return self._con       

    @staticmethod
    def time_from_string(stim):
        """Convert DB time string to int holding unix time."""
        myname = "MonDbReader::time_from_string:"
        if stim is None: return 0
        if type(stim) is not str:
            print(f"""{myname}: ERROR: Cannot convert type {type(stim)}.""")
            assert(False)
        assert(type(stim) is str)
        stimarr = stim.split('.')
        if len(stimarr) > 2: raise Exception(f"""{myname}: Invalid input string: {stim}""")
        tim = datetime.datetime.strptime(stimarr[0], '%Y-%m-%d %H:%M:%S').timestamp()
        if len(stimarr) > 1:
            tim = tim + float('0.' + stimarr[1])
        return tim

    @staticmethod
    def string_from_time(itim):
        """Convert unix time to DB time string."""
        assert(type(itim) is int or type(itim) is float)
        fmt = '%Y-%m-%d %H:%M:%S'
        if type(itim) is float: fmt = fmt + '.%f'
        return datetime.datetime.fromtimestamp(itim).strftime(fmt)

    def table_names(self):
        """Return the table names."""
        return list(self._tables.keys())

    def tables(self, lev=0):
        """Fetch the dictionary of all tables or display info about the tables.
          lev = 0: Return table distionary.
          lev = 1: Display table names.
          lev = 2: Columns are displayed.
          lev = 3: Columns and content are displayed.
        """
        if lev > 0:
            print(f"""DB {self.filename} has {len(self._tables)} tables""")
        line = '*******************************************************'
        add_closing_line = False
        if lev==1: print(f"{'Table':>15}{'Nrow':>10}{'Ncol':>10}")
        for tnam in self.table_names():
            if lev == 1:
                tab = self._tables[tnam]
                nrow = len(tab.index)
                ncol = len(tab.columns)
                print(f"{tnam:>15}{nrow:>10}{ncol:>10}")
            elif lev > 1:
                print(line)
                add_closing_line = True
                self.table(tnam, lev)
        if add_closing_line: print(line)
        if lev==0: return self._tables

    def table(self, tnam, lev=0):
        """
        Fetch table tnam or display info about it according to lev.
        lev = 0: Return the table.
        lev = 1: Size or message shown if table does not exist.
        lev = 2: Columns are displayed.
        lev = 3: Columns and content are displayed.
        """
        if tnam not in self._tables:
            if lev > 0:
                print(f"""Table {tnam} does not exist.""")
            return None
        tab = self._tables[tnam]
        if lev==0: return tab
        nrow = len(tab.index)
        ncol = len(tab.columns)
        print(f"""Table {tnam} has {nrow} rows and {ncol} columns""")
        print('Column names:')
        if lev > 1:
            for col in tab:
                print(str(tab.dtypes[col]).rjust(10), ' ', col)
        if lev > 2:
            display(tab)

    def fix_runs(self):
        """Replace run IDs with indices. Drop rows for IDs that dot not appear in task table."""
        myname = self.__class__.__name__ + "::fix_runs"
        if self.dbg: print(f"""{myname}: Fixing runs.""")
        if 'runs' in self.fixed:
            print(f"""{myname}: Run IDs are already fixed.""")
            return
        col = 'workflow_name'
        assert(len(self.run_ids) == 0)
        tnam = 'task'
        if tnam not in self.table_names(): raise Exception(f"{myname}: Table {tnam} not found.")
        tab = self[tnam]
        col = 'run_id'
        cols = tab.columns.values.tolist()
        if col not in cols: raise Exception(f"{myname}: Column {col} not found in table {tnam}.")
        self.run_ids = list(dict.fromkeys(self[tnam][col]))  # Removes duplicates and maintains order.
        if len(self.run_ids) == 0: raise Exception(f"{myname}: No run IDs found in column {col} of table {tnam}.")
        nrid = len(self.run_ids)
        runIdToIndex = {}
        for idx in range(nrid):
            runIdToIndex[self.run_ids[idx]] = idx
        for tnam in self.table_names():
            tab = self.table(tnam)
            nrow = len(tab)
            # Remove rows with run_id not in task.
            qry = col + ' in ' + str(self.run_ids)
            tab.query(qry, inplace=True)
            nrem = nrow - len(tab)
            if nrem > 0:
                self.remove_counts[tnam] += nrem
                tab.reset_index(inplace=True)
            tab[col].replace(runIdToIndex, inplace=True)
            tab.rename(columns={'run_id':'run_idx'}, inplace=True)
            #if tnam == 'workflow' and 'index' in tab.columns.values.tolist():
            #    tab['index'] = tab[col]
        self.fixed.append('runs')

    def fix_workflows(self):
        """Replace workflow names with indices and fill sel.workflow_names with those names."""
        myname = self.__class__.__name__ + "::fix_workflows"
        if self.dbg: print(f"""{myname}: Fixing workflows.""")
        if 'workflows' in self.fixed:
            print(f"""{myname}: Workflows are already fixed.""")
            return
        col = 'workflow_name'
        tab = self.table('workflow')
        nameToIdx = {}
        for wnam in tab[col]:
            if wnam not in self.workflow_names:
                nameToIdx[wnam] = len(self.workflow_names)
                self.workflow_names.append(wnam)
        tab[col].replace(nameToIdx, inplace=True)
        tab.rename(columns={col:'wf_idx'}, inplace=True)
        # Drop the workflow version. It is just the start time.
        col = 'workflow_version'
        if col in tab.columns.values.tolist():
            tab.drop(columns=[col], inplace=True)
        self.fixed.append('workflows')

    def fix_times(self):
        """Replace time strings with unix seconds. None is left as None."""
        myname = self.__class__.__name__ + "::fix_times"
        if 'times' in self.fixed:
            if self.dbg: print(f"""{myname} Times are already fixed.""")
            return
        if self.dbg: print(f"""{myname}: Fixing times.""")
        if 'runs' not in self.fixed:
            if self.dbg >= 2: print(f"""{myname}: Fixing runs.""")
            self.fix_runs()
        self.t0 = self.time_from_string(self.table('workflow').at[0, 'time_began'])
        for tnam in self.table_names():
            tab=self.table(tnam)
            for cnam in tab.columns:
                if cnam[0:4]=='time' or cnam[0:9]=='task_time' or cnam[0:13]=='task_try_time':
                    if self.dbg >=2: print(f"""{myname}: Fixing column {cnam} in table {tnam}""")
                    tab[cnam] = tab[cnam].apply(lambda x: x if (x is None) else self.time_from_string(x) - self.t0)
        # If any workflow end times have not been recorded, use the last entry in the resource table.
        # Also add each worflow time range to self.workflow_time_ranges
        wkf = self.table('workflow')
        nwkf = len(wkf)
        cwkf = 'time_completed'
        res = self.table('resource')
        cres = 'timestamp'
        for iwkf in range(0, nwkf):
            if wkf[cwkf][iwkf] is None:
                tmax = max(res[cres])
                if iwkf+1 < nwkf:
                    qry = cres + '<' + str(wkf['time_began'][iwkf+1])
                    ttmp = max(res.query(qry)[cres])
                    if ttmp is not None: tmax = ttmp
                wkf.at[iwkf, cwkf] = tmax
            t1 = wkf['time_began'][iwkf]
            t2 = wkf['time_completed'][iwkf]
            if self.dbg >= 3: print(f"""{myname}: Time range for run {iwkf} is [{t1}, {t2}].""")
            self.workflow_time_ranges.append((t1, t2))
            for itp in range(2):
              self.taskprocs[itp].append({})
        # Set flag.
        self.fixed.append('times')

    def fix_tasks(self):
        """
        Task function names are assigned indices and the following members are filled:
          task_names[tidx] - Task function name for each task index.
          task_name_counts[tidx] - Number of task IDs associated with each task index.
          task_index[ridx][tid] - Task index for each run index and task ID.
        The task table column 'task_func_name' is replaced with 'task_idx' holding the task index.
        Task indices ('task_idx') to all tables that have task IDs ('task_id').
        """
        myname = self.__class__.__name__ + "::fix_tasks"
        if len(self.task_names):
            if self.dbg: print(f"""{myname}: Tasks are already fixed.""")
            return
        if not len(self.run_ids): self.fix_runs()
        if self.dbg: print(f"""{myname}: Fixing tasks.""")
        self.task_names = []
        self.task_name_counts = []
        self.task_logs = []
        task_idxs = []   # New column
        tab = self.table('task')
        for dum in self.run_ids: self.task_index.append([])
        run_idx = 0
        task_id = 0
        for row in tab.itertuples():
            tnam = row.task_func_name
            if row.run_idx != run_idx:
                run_idx = row.run_idx
                task_id = 0
            assert( task_id == row.task_id )   # Assume task_id numbering follws that of rows
            if tnam not in self.task_names:
                idx = len(self.task_names)
                self.task_names.append(tnam)
                self.task_name_counts.append(1)
                self.taskIndexFromName[tnam] = idx
            else:
                idx = self.taskIndexFromName[tnam]
                self.task_name_counts[idx] = self.task_name_counts[idx] + 1
            task_idxs.append(idx)
            assert( len(self.task_index[run_idx]) == task_id)
            self.task_index[run_idx].append(idx)
            self.task_logs.append(row.task_stderr)
            task_id = task_id + 1
        # In 'task' replace column task_func_name with task_idx
        cnam = 'task_func_name'
        icol = tab.columns.get_loc(cnam)
        tab.insert(icol, 'task_idx', task_idxs)
        tab.drop(labels=cnam, axis=1, inplace=True)
        # Add 'run_idx' to all the other tables thaat have 'run_id'.
        for tnam in self.table_names():
            tab = self.table(tnam)
            cnams = tab.columns.values.tolist()
            if tnam != 'task' and 'task_id' in cnams:
                if self.dbg > 1: print(f"""{myname}: Adding task_idx to table {tnam}""")
                assert('run_idx' in cnams)    # fix_runs should have added this
                vals = []
                for row in tab.itertuples():
                    run_idx = row.run_idx
                    task_id = row.task_id
                    vals.append(self.task_index[run_idx][task_id])
                icol = tab.columns.get_loc('task_id') + 1
                tab.insert(icol, 'task_idx', vals)
            else:
                if self.dbg > 2: print(f"""{myname}: NOT adding task_id in table {tnam}""")
        # set flag indicating tasks have been fixed.
        self.fixed.append('tasks')

    def taskproc(self, runidx, taskid, dodelta=True, build=True):
        """
        Return the taskproc table for the given run index and task ID.
        Build the dictionary if build is true and it does not already exist.
        """
        myname = self.__class__.__name__ + "::task_procsum"
        # Check if summary already exists.
        if runidx < len(self.taskprocs[dodelta]) and taskid in self.taskprocs[dodelta][runidx]:
            return self.taskprocs[dodelta][runidx][taskid]
        # Make sure tasks and times are already fixed so we have task indices and the starting time.
        if 'tasks' not in self.fixed:
            if self.dbg >= 2: print(f"""{myname}: Fixing tasks.""")
            self.fix_tasks()
        if len(self.workflow_time_ranges) == 0:
            if self.dbg >= 2: print(f"""{myname}: Fixing times.""")
            self.fix_times()
        # Check run ID is in range.
        if runidx >= len(self.workflow_time_ranges):
            if self.dbg: print(f"""{myname}: ERROR: Run index {runidx} is out of range.""")
            return None
        # Columns for which we keep.
        colkeeps = ['timestamp', 'run_idx', 'task_idx', 'task_id', 'try_id']
        # Columns that we add.
        colnews = ['toff', 'proc_time_clock', 'nsam', 'isam']
        # Columns for which we keep but rename: psutil_process --> proc
        colproc_rens = [
                'psutil_process_pid',
                'psutil_process_status',
                #20220304: 'psutil_process_cpu_percent',
                'psutil_process_memory_percent',
                'psutil_process_memory_resident',
                'psutil_process_memory_virtual'
            ]
        colproc_rens_renamed = []
        for cnam in colproc_rens:
            colproc_rens_renamed.append('proc' + cnam[14:])
        if self.dbg >= 2: print(f"""{myname}: Renamed columns: {colproc_rens_renamed}.""")
        # Columns for which we may return deltas and renamed to procdel with deltas or proc without.
        colproc_dels = [
             'psutil_process_children_count',
             'psutil_process_time_user',
             'psutil_process_time_system',
             'psutil_process_disk_read',
             'psutil_process_disk_write'
        ]
        if dodelta: prefix = 'procdel'
        else: prefix = 'proc'
        colproc_dels_renamed = []
        for cnam in colproc_dels:
            colproc_dels_renamed.append(prefix + cnam[14:])
        if self.dbg >= 2: print(f"""{myname}: Delta columns: {colproc_dels_renamed}.""")
        # Select rows with given IDs.
        res = self.table('resource')
        assert(res is not None)
        qry = f"""run_idx=={runidx} and task_id=={taskid}"""
        if self.dbg >= 3: print(f"""{myname}: Processing '{qry}'.""")
        # Select entries for this task, order by timestamp and reset indices.
        olddf = res.query(qry).sort_values(by=['timestamp'], ignore_index=True)
        if len(olddf) == 0:
            if self.dbg: print(f"""{myname}: ERROR: No resouce entries match {qry}.""")
            return None
        if self.dbg >= 2: print(f"""{myname}: Row count for ({runidx}, {taskid}) is {len(olddf)}.""")
        # Check the monitoring period is constant.
        sint = 'resource_monitoring_interval'
        dt = self.monitoring_interval
        if dt is None:
            idx0 = olddf.index[0]
            dt = olddf.at[idx0, sint]
            self.monitoring_interval = dt
        if len(olddf.query(f"""{sint}!={dt}""")):
            raise Exception(f"""{myname}: ERROR: Inconsistent {sint} values.""")
        # Assuming binning starts at 0.0, check if adjacent samples are not in adjacent bins.
        # If so try to find an offset that fixes this if we bin in timestamp + toff.
        toff = 0.0
        dtoff = min(1.0, 0.1*dt)
        while len(olddf) > 2:
            maxdif = max(olddf.timestamp.add(toff).mod(dt).diff()[2:].abs())
            if maxdif < dt/2: break
            if toff > dt/2 + 1:
                if self.dbg>2: print(f"""{myname}: WARNING: Unable to find offset for run {runidx} task {taskid}. Using 0.0.""")
                ++self.nwarnNoOffset
                toff = 0.0
                break
            toff = toff + dtoff
        # Add clock time = time since job started.
        ttry = self.table('try')
        ttryproc = ttry.query(qry)
        t0 = ttryproc['task_try_time_running'].squeeze()
        timdf = olddf['timestamp'] - t0
        timlab = 'procsum_time_clock'
        if dodelta:
            if self.dbg >= 4: print(f"""{myname}: Evaluating deltas.""")
            # To get the deltas, keep the first row and use diff for the remainder.
            # There is a missing contribution from process activity afte the last sampling.
            deldf = olddf[colproc_dels].diff()
            deldf.iloc[0] = olddf[colproc_dels].iloc[0]
            tsav = timdf.iloc[0]
            timdf = timdf.diff()
            timdf.iloc[0] = tsav
            timlab = 'procdel_time_clock'
        else:
            deldf = olddf[colproc_dels]
        # Build the ouput dataframe.
        newdf = olddf[colkeeps].copy()
        newdf['toff'] = toff
        newdf[timlab] = timdf
        nsam = len(olddf)
        newdf['nsam'] = nsam
        newdf['isam'] = range(0, nsam)
        newdf[colproc_rens_renamed] = olddf[colproc_rens]
        newdf[colproc_dels_renamed] = deldf[colproc_dels]
        self.taskprocs[dodelta][runidx][taskid] = newdf
        return newdf

    def build_procsum(self, dodelta=True):
        """Build the procsum table which sums resources in bins of sampling time."""
        myname = self.__class__.__name__ + "::build_procsum"
        if dodelta:
            msg = "with deltas"
            savename = 'procsumDelta'
        else:
            msg = "without deltas"
            savename = 'procsumNoDelta'
        if savename in self.table_names():
            if self.dbg >= 1: print(f"""{myname}: Table procsum {msg} was already built.""")
            self._tables['procsum'] = self._tables[savename]
            return self.table('procsum')
        if self.dbg >= 1: print(f"""{myname}: Building table procsum {msg}.""")
        # Columns for which we keep one value in the summary.
        colkeeps = ['run_idx']
        # Columns for which we sum values in each time bin.
        if dodelta:
            # Make sure we have the workflow times.
            if len(self.workflow_time_ranges) == 0:
                if self.dbg >= 2: print(f"""{myname}: Fixing times.""")
                self.fix_times()
            colproc_sums = [
                    #20220304 'proc_cpu_percent',
                    'proc_memory_percent',
                    'proc_memory_resident',
                    'proc_memory_virtual',
                    'procdel_time_clock',
                    'procdel_time_user',
                    'procdel_time_system',
                    'procdel_disk_read',
                    'procdel_disk_write'
                ]
            sreps = ['proc_', 'procdel_']
        else:
            colproc_sums = [
                    #20220304 'psutil_process_cpu_percent',
                    'psutil_process_memory_percent',
                    'psutil_process_memory_resident',
                    'psutil_process_memory_virtual'
                ]
            sreps = ['psutil_process_']
        if self.dbg >= 2: print(f"""{myname}: Summed columns: {colproc_sums}.""")
        # Loop over workflow runs and append the summary info for each to the procsum table.
        nrun = len(self.workflow_time_ranges)
        if nrun == 0:
            if self.dbg: print(f"""{myname}: No workflow runs found.""")
            return
        if self.dbg: print(f"""{myname}: Building procsum for {nrun} workflow runs.""")
        if dodelta:
            mrgdfs = []
            for irun in range(0, nrun):
                tids = set(self.table('resource').query(f"""run_idx=={irun}""")['task_id'].tolist())
                if self.dbg >= 3: print(f"""{myname}:   Workflow {irun} has {len(tids)} task IDs.""")
                for tid in tids:
                    dbgsav = self.dbg
                    self.dbg = 0
                    mrgdfs.append(self.taskproc(irun, tid))
                    self.dbg = dbgsav
            olddf = pandas.concat(mrgdfs)
            pidnam = 'proc_pid'
        else:
            res = self.table('resource')
            self.monitoring_interval = res.at[0, 'resource_monitoring_interval']
            olddf = res[ ['timestamp', 'psutil_process_pid'] + colkeeps + colproc_sums ]
            pidnam = 'psutil_process_pid'
        dt = self.monitoring_interval
        assert(dt is not None)
        t1 = int(olddf.timestamp.min()/dt)*dt
        t2 = int(olddf.timestamp.max()/dt + 1.0)*dt
        rngs = numpy.arange(t1, t2, dt)
        bins = pandas.cut(olddf.timestamp, rngs)
        # First handle the keeps and sums.
        # Aggregate: evaluate count, first, or sum for various columns.
        agdict = {'timestamp':['count'], pidnam:['nunique']}
        for col in colkeeps: agdict[col] = ['first']
        for col in colproc_sums: agdict[col] = 'sum'
        newdf = olddf.groupby(bins).agg(agdict)
        cnams = ['nval', 'nproc'] + colkeeps
        for oldnam in colproc_sums:
            newnam = oldnam
            for srep in sreps:
                if oldnam[0:len(srep)] == srep:
                    newnam = oldnam.replace(srep, 'procsum_')
                    break;
            cnams.append(newnam)
        #print(olddf.columns)
        #print(newdf.columns)
        #rint(cnams)
        newdf.columns = cnams
        # Drop rows without any processes.
        newdf.insert(0, 'timestamp', rngs[0:len(rngs)-1] + dt/2)
        newdf.index = range(0, len(newdf))
        newdf.query('nproc>0', inplace=True)
        self._tables['procsum'] = newdf
        self._tables[savename] = newdf
        if self.nwarnNoOffset:
            print(f"{myname}: WARNING: Count of tasks with no-offset warning: {self.nwarnNoOffset}")
        return newdf

    def fix(self, dodelta =False):
        """Fix everything: runs, workflows, times and tasks."""
        self.fix_runs()
        self.fix_workflows()
        self.fix_times()
        self.fix_tasks()
        self.build_procsum(dodelta)

    def workflow_time_range(self, iwkf, unit='second'):
        """Return the time range for workflow iwkf."""
        wkf = self.table('workflow')
        if iwkf >= len(wkf): return None
        if   unit == 'second': fac = 1.0
        elif unit == 'minute': fac = 1.0/60.0
        elif unit ==   'hour': fac = 1.0/3600.0
        elif unit ==    'day': fac = 1.0/(24*3600.0)
        else: return None
        t1 = fac*wkf['time_began'][iwkf]
        t2 = fac*wkf['time_completed'][iwkf]
        return (t1, t2)

    def taskcounts(self, state=None, runidx=0, delt=1):
        """
        Return a dataframe time:task_idx for a give state and run index
        holding the total nuber of task tries that have reached the state.
        States are ['launched', 'running', 'returned']
        """
        nrun = len(self.workflow_names)
        if nrun == 0: return
        ntsk = len(self.task_names)
        snams = ['launched', 'running', 'returned']
        tnams = list(range(ntsk))
        cnams = ['time'] + tnams
        nsta = len(snams)
        if len(self._taskcounts) != nrun:
            self.taskcount_delt = delt
            tcs = [{}]*nrun
            # Loop over runs and create zeroed task counters.
            for irun in range(nrun):
                t1 = self.workflow_time_ranges[irun][0]
                t2 = self.workflow_time_ranges[irun][1]
                toff = t1
                ntim = int((t2-t1)/delt) + 1
                df = pandas.DataFrame(0, index=range(ntim), columns=cnams)
                df['time'] = numpy.arange(t1-toff, t2-toff+10*delt, delt)[0:ntim]
                for snam in snams:
                    tcs[irun][snam] = df.copy()
            # Loop over tries and fill the counters.
            for row in self.table('try').itertuples():
                for snam in snams:
                    mytcs = tcs[row.run_idx][snam]
                    cnam_try = 'task_try_time_' + snam
                    cnam_tcs = row.task_idx
                    time = getattr(row, cnam_try)
                    sel = mytcs['time'] > time
                    mytcs.loc[sel, cnam_tcs] += 1
            # Add a column with sum over tasks.
            for irun in range(nrun):
                for snam in snams:
                    mytcs = tcs[irun][snam]
                    mytcs['all'] = mytcs[tnams].sum(1)
            self._taskcounts = tcs
        if state is None: return
        return self._taskcounts[runidx][state]
        

import unittest

class TestMondDbReader(unittest.TestCase):
    """
    Class to test MonDbReader. Run with
      > nosetests -v mondb.py
    or
      > nosetests -v --pdb mondb.py  # Drops into debugger on error or exception.
    """
    myname = 'TestMonDbReader:ctor'
    badfnam = "nosuchfile"
    fnam = "/global/homes/d/dladams/desc/test6/monitoring.db"
    ntab = 7
    dbr = None
    allfixes = ['runs', 'workflows', 'times', 'tasks', 'procsum']
    print(f"{myname}: Done.")

    def test_good(self):
        self.assertEqual(1,1)

    #def test_bad(self):
    #    self.assertEqual(1,3)

    def test_help(self):
        print(help(MonDbReader))

    def test_bad(self):
        baddbr = MonDbReader(self.badfnam, fix=False)
        self.assertEqual(baddbr.filename, self.badfnam)
        self.assertEqual(len(baddbr.tables()), 0)
        try:
            baddbr.fix()
            print('Expected exception was not raised.')
        except:
            pass

    def test_open(self):
        self.dbr = MonDbReader(self.fnam, fix=False)
        self.assertEqual(self.dbr.filename, self.fnam)
        self.assertEqual(len(self.dbr._tables), self.ntab)
        self.assertEqual(len(self.dbr.tables()), self.ntab)
        self.assertEqual(len(self.dbr.fixed), 0)

    def test_len(self):
        self.test_open()
        self.assertEqual(len(self.dbr), self.ntab)

    def test_table_names(self):
        self.test_open()
        self.assertEqual(len(self.dbr.table_names()), self.ntab)

    def test_tables(self):
        self.test_open()
        self.assertEqual(len(self.dbr.tables()), self.ntab)

    def test_fix_runs(self):
        self.test_open()
        dbr = self.dbr
        self.assertFalse('runs' in dbr.fixed)
        self.assertTrue(len(dbr.run_ids) == 0)
        dbr.fix_runs()
        self.assertTrue('runs' in dbr.fixed)
        self.assertTrue(len(dbr.run_ids) > 0)

    def test_fix_workflows(self):
        self.test_open()
        dbr = self.dbr
        self.assertFalse('workflows' in dbr.fixed)
        self.assertEqual(len(dbr.workflow_names), 0)
        dbr.fix_workflows()
        self.assertTrue('workflows' in dbr.fixed)
        self.assertEqual(len(dbr.workflow_names), 1)

    def test_fix_times(self):
        self.test_open()
        dbr = self.dbr
        self.assertFalse('times' in dbr.fixed)
        self.assertEqual(dbr.t0, 0)
        dbr.fix_times()
        self.assertTrue('times' in dbr.fixed)
        self.assertNotEqual(dbr.t0, 0)
        st0 = dbr.string_from_time(dbr.t0)
        self.assertEqual(dbr.time_from_string(st0), dbr.t0)

    def test_fix_tasks(self):
        self.test_open()
        dbr = self.dbr
        tnam = 'task'
        cnam1 = 'task_func_name'
        cnam2 = 'task_idx'
        self.assertFalse('runs' in dbr.fixed)
        self.assertIn(tnam, dbr.table_names())
        self.assertIn(cnam1, dbr[tnam].columns)
        self.assertNotIn(cnam2, dbr[tnam].columns)
        self.assertEqual(len(dbr.task_names), 0)
        self.assertEqual(len(dbr.task_name_counts), 0)
        dbr.fix_tasks()
        self.assertTrue('runs' in dbr.fixed)
        self.assertTrue('tasks' in dbr.fixed)
        self.assertNotIn(cnam1, dbr[tnam].columns)
        self.assertIn(cnam2, dbr[tnam].columns)
        self.assertNotEqual(len(dbr.task_names), 0)
        self.assertNotEqual(len(dbr.task_name_counts), 0)

    def test_build_procsum(self):
        self.test_open()
        dbr = self.dbr
        tnam = 'procsum'
        self.assertFalse(tnam in dbr.fixed)
        dbr.build_procsum()
        self.assertTrue(tnam in dbr.fixed)

    def test_fix(self):
        self.test_open()
        dbr = self.dbr
        self.assertEqual(dbr.fixed, [])
        dbr.fix()
        self.assertEqual(dbr.fixed, self.allfixes)

    def test_ctor(self):
        dbr = MonDbReader()
        self.assertEqual(dbr.fixed, self.allfixes)

    def test_workflow_time_range(self):
        dbr = MonDbReader()
        iwkf = 1
        unit = "hour"
        ran = dbr.workflow_time_range(1, 'hour')
        print(f"""'Range for workflow {iwkf} is {ran} {unit}""")

if __name__ == '__main__':
    unittest.main()

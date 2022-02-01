#!/usr/bin/env python3

"""
Script to collect system-level resource info indluding CPU, memory, disk and netwwork.
"""

import psutil
import subprocess
import os
import time
from threading import Thread

def none():
    """Function that always returns none."""
    return None

class Notify:
    """Class to notify reporter when to exit."""
    def __init__(self, val =None):
        self.value = val
    def __call__(self):
        return self.value
    def set(self, val =None):
        self.value = val

class Params:
    """Class that holds the parameters for reporter."""
    fnam = 'sysmon.csv'
    dt = 10
    check = none
    timeout = 0
    subcom = ''
    dbg = 1
    thr = False
    def __init__(self):
        """Set defaults."""
        self.fnam = 'sysmon.csv'
        self.dt = 10
        self.check = none
        self.timeout = 0
        self.subcom = ''
        self.dbg = 1
        self.thr = False
    def update(self, vals):
        for key in vals:
            val = vals[key]
            if      key == 'fnam': self.fnam = val
            elif      key == 'dt': self.dt = val
            elif   key == 'check': self.check = val
            elif key == 'timeout': self.timeout = val
            elif  key == 'subcom': self.subcom = val
            elif     key == 'dbg': self.dbg = val
            elif     key == 'thr': self.thr = val
            else: raise KeyError(f"Invalid reporter parameter name: {key}")


def reporter(fnam =Params.fnam, dt =Params.dt, check =Params.check, timeout =Params.timeout,
             subcom =Params.subcom, dbg=Params.dbg, thr=Params.thr):
    """
    Report system parameters.
         fnam - Output file name ['sysmon.csv'].
           dt - Polling time interval in seconds [10].
       subcom - If not empty, then subcom is run as a subprocess
                and polling ceases when that process exits [''].
        check - If not None polling ceases when check() returns anything
                except None [None].
      timeout - If nonzero, polling ceases after timeout seconds [0].
          dbg - Lof message level.
          thr - If true, reporter is run in a thread and this returns immediately.
    """
    myname = 'sysmon.reporter'
    if dbg > 1:
        print(f"     fnam: {fnam}")
        print(f"       dt: {dt}")
        print(f"   subcom: {subcom}")
        print(f"    check: {check}")
        print(f"  timeout: {timeout}")
        print(f"      dbg: {dbg}")
        print(f"      thr: {thr}")
    # Make sure the output directory exists.
    dnam = os.path.dirname(fnam)
    if len(dnam):
        if not os.path.isdir(dnam):
            if dbg > 0: print(f"{myname}: Creating output directory {dnam}")
            os.makedirs(dnam)
    # If this is a thread request, create and start the thread.
    if thr:
        if dbg > 0: print(f"{myname}: Starting thread.")
        args=(fnam, dt, check, timeout, subcom, dbg, False)
        t = Thread(target=reporter, args=args)
        t.start()
        return t
    version = '0.00.02'
    if dbg > 0: print(f"{myname}: Starting reporter version {version}")
    if dbg > 0: print(f"{myname}: Logging to {fnam}")
    subproc = None
    # Specify which fields are to be recorded.
    # cpu_time = user + system + idle + ...
    # memory fields are in GB = 2e30 byte
    # Memory and I/0 are all incremental, i.e. the change since the sampling.
    keys = ['time', 'cpu_count', 'cpu_percent']
    keys += ['cpu_user', 'cpu_system', 'cpu_idle', 'cpu_iowait', 'cpu_time']
    keys += ['mem_total', 'mem_available', 'mem_swapfree']    # Should we add swap total, in and out?
    #keys += ['dio_readcount', 'dio_writecount']
    keys += ['dio_readsize', 'dio_writesize']
    keys += ['nio_readsize', 'nio_writesize']
    hdrline = keys[0]
    for key in keys[1:]:
        hdrline += ',' + key
    if os.path.exists(fnam) and os.path.getsize(fnam):
        firstline = next(open(fnam, 'r')).rstrip()
        if firstline != hdrline:
            raise Exception(f"""File {fnam} does not have the expected header.""")
        needheader = False
    else:
        needheader = True
    try:
        csv_file = open(fnam, "a")
    except e:
        print(f"{myname}: ERROR: {e}")
    else:
        if dbg > 2: print(f"{myname}: Reporting...")
        if needheader:
            print(hdrline, file=csv_file, flush=True)
            needheader = False
        cptlast = None
        diolast = None
        niolast = None
        time0 = time.time()
        npoll = 0
        while True:
            d = {}
            now = time.time()
            d['time'] = now
            d['cpu_count'] = psutil.cpu_count()
            d['cpu_percent'] = psutil.cpu_percent()
            # user, nice, system, idle, iowait, irq, softirq, steal, guest, guest_nice
            cpt = psutil.cpu_times()
            mem = psutil.virtual_memory()
            swap = psutil.virtual_memory()
            dio = psutil.disk_io_counters()
            nio = psutil.net_io_counters()
            cptsum = sum(cpt)
            if cptlast is None:
                cptlast = cpt
                cptsumlast = cptsum
            d['cpu_user']   = cpt.user   - cptlast.user
            d['cpu_system'] = cpt.system - cptlast.system
            d['cpu_idle']   = cpt.idle   - cptlast.idle
            d['cpu_iowait'] = cpt.iowait - cptlast.iowait
            d['cpu_time']   = cptsum     - cptsumlast
            cptlast = cpt
            cptsumlast = cptsum
            gb = 2**30
            d['mem_total'] = mem.total/gb
            d['mem_available'] = mem.available/gb
            d['mem_swapfree'] = swap.free/gb
            if diolast is None:
                diolast = dio
            d['dio_readcount']  =  dio.read_count  - diolast.read_count
            d['dio_writecount'] =  dio.write_count - diolast.write_count
            d['dio_readsize']   = (dio.read_bytes  - diolast.read_bytes)/gb
            d['dio_writesize']  = (dio.write_bytes - diolast.write_bytes)/gb
            diolast = dio
            if niolast is None:
                niolast = nio
            d['nio_readsize']   = (nio.bytes_recv - niolast.bytes_recv)/gb
            d['nio_writesize']  = (nio.bytes_sent - niolast.bytes_sent)/gb
            niolast = nio
            # Write the selected fields.
            sep = ''
            line = ''
            fmt = '.3f'
            for key in keys:
                line = f"""{line}{sep}{d[key]:{fmt}}"""
                sep = ','
            print(line, file=csv_file, flush=True)
            npoll += 1
            status = none
            if len(subcom):
                # If we are give a subprocess command, make sure that we poll before
                # the process starts and after it ends.
                if subproc is None:
                    subproc = subprocess.Popen(subcom.split())
                else:
                    if subproc.poll() is not None:
                        status = subproc.returncode
                        reason = f'subprocess terminated with status {subproc.returncode}'
                        break
            checkval = None if check is None else check()
            if dbg > 2: print(f'{myname}: Check returns {checkval}')
            if checkval is not None:
                reason = f'check returned {checkval}'
                break
            if timeout > 0 and now - time0 > timeout:
                reason = f'total time exceeded {timeout} sec'
                break
            if dbg > 1: print(f'{myname}: Sleeping...')
            time.sleep(dt)
    finally:
        csv_file.close()

    if dbg > 0: print(f"{myname}: Polling terminated because {reason}")
    if dbg > 0: print(f"{myname}: Poll count is {npoll}")
    return 0

def reporter_from_string(scfg =''):
    myname = 'reporter_from_string'

def main_reporter():
    """
    Main function wrapper for reporter.
    First argument is the configuration to exec, e.g.
      'fnam="syslog.csv";dt=5;timeout=3600'
    Remaining argumens are the subcommand, e.g.
      run-my-jobs arg1 arg2
    The reporter will start logging and and continue until the command returns.
    """
    import sys
    print('Running the desc-sysmon-reporter')
    myname = 'main_reporter'
    pars = Params()
    cfg = sys.argv[1] if len(sys.argv) else ''
    print(f"{myname}: Configuring with '{cfg}'")
    glos = {}
    vals = {}
    if len(cfg): exec(cfg, glos, vals)
    pars.update(vals)
    if len(sys.argv) > 1: pars.subcom = " ".join(sys.argv[2:])
    reporter(pars.fnam, pars.dt, pars.check, pars.timeout, 
             pars.subcom, pars.dbg, pars.thr)

#if __name__ == "__main__":
#    main_reporter()

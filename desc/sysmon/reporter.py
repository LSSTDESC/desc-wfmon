#!/usr/bin/env python3

"""
Script to collect system-level resource info indluding CPU, memory, disk and netwwork.
"""

import psutil
import subprocess
import os
import time

def none():
    return None

def reporter(fnam ='sysmon.csv', dt =10,  check =none, timeout =0, comargs=[]):
    """
    Report system parameters.
    Output file is fnam.
    One entry is added every dt seconds.
    If comargs is not empty, then that command is run as a subprocess and polling
    ceases when the process terminates.
    continues until that process ends.
    Polling also ceases when poll() is not None or
    the time elapsed time exceeds nonzero timeout.
    """
    myname = 'sysmon.reporter'
    version = '0.00.01'
    print(f"{myname}: Starting reporter version {version}")
    print(f"{myname}: Logging to {fnam}")
    print(f"{myname}: Logging to {fnam}")
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
    with open(fnam, "a") as csv_file:
        if needheader:
            print(hdrline, file=csv_file, flush=True)
            needheader = False
        print('', file=csv_file, flush=True)
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
            if len(comargs):
                # If we are give a subprocess command, make sure that we poll before
                # the process starts and after it ends.
                if subproc is None:
                    subproc = subprocess.Popen(comargs)
                else:
                    if subproc.poll() is not None:
                        status = subproc.returncode
                        reason = f'subprocess terminated with status {subproc.returncode}'
                        break
            checkval = check()
            if checkval is not None:
                reason = f'check returned {checkval}'
                break
            if timeout > 0 and now - time0 > timeout:
                reason = f'total time exceeded {timeout} sec'
                break
            time.sleep(10)

    print(f"{myname}: Polling terminated because {reason}")
    print(f"{myname}: Poll count is {npoll}")
    return 0

if __name__ == "__main__":
    reporter(**argv)


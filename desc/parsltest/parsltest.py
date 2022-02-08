# parsltest.py
#
# David Adams
# January 2022

import parsl
import time
import psutil
import sys
import os
import desc.sysmon

def load_config(max_workers =4, dsam =10):
    lcom = parsl.executors.high_throughput.executor.DEFAULT_LAUNCH_CMD
    rcfg = f"dbg=2;dt={dsam};fnam='runinfo/sysmon.csv';log='runinfo/sysmon.log'"
    lcom = 'desc-sysmon-reporter "' + rcfg + '" ' + lcom
    config = parsl.config.Config(
       executors=[
           parsl.HighThroughputExecutor(
               launch_cmd = lcom,
               label="local_htex",
               cores_per_worker=1,
               max_workers=max_workers,
               address=parsl.addresses.address_by_hostname(),
           )
       ],
       monitoring=parsl.monitoring.monitoring.MonitoringHub(
           hub_address=parsl.addresses.address_by_hostname(),
           hub_port=55055,
           monitoring_debug=False,
           resource_monitoring_interval=dsam,
       ),
       strategy=None
    )
    parsl.load(config)

@parsl.python_app
def myjob(name, trun):
    import time
    print(f"""Running job {name} for {trun} seconds""")
    time.sleep(trun)
    return f"""Finished job {name}"""

@parsl.bash_app
def mybash(name, trun, memmax):
    return f"""desc-cpuburn {name} {trun} {memmax} 2>&1 >{name}.log; echo Finished job {name}"""

def parsltest(njob =4, sfac =10, memmax =10, clean =True, twait =5, max_workers =4, dsam =10):
    print('Welcome to parsltest')
    print(f"""Parsl version is {parsl.version.VERSION}""")
    print(f"""Desc-sysmon version is {desc.sysmon.__version__}""")
    msg = desc.sysmon.Notify()
    #thr = desc.sysmon.reporter('out/sysmon.csv', check=msg, dbg=3, thr=True)
    msg = f"Running {njob} jobs for {sfac}"
    if njob > 1:
        msg += f", {2*sfac}"
        if njob > 2:
            if njob > 3: msg += ', ...'
            msg += f", {njob*sfac}"
    msg += ' seconds.'
    print(f"""Running {njob} jobs for {sfac}, {2*sfac}, ..., {njob*sfac} seconds.""")
    print(f"""Job memory limit is {memmax} GB.""")
    print(f"""Number of workers: {max_workers}.""")
    print(f"""Monitor sampling time: {dsam} seconds.""")
    if clean: os.system('./clean')
    #parsl.clear()
    load_config(max_workers, dsam)
    jobs = []
    jobsDone = []
    for ijob in range(1,njob+1):
        jobs.append( mybash(f'job{ijob:02}', sfac*ijob, memmax) )
    showio = False
    if showio: print(psutil.disk_io_counters())
    if showio: print(psutil.net_io_counters())
    while len(jobs):
        ijob = 0
        while ijob < len(jobs):
            job = jobs[ijob]
            if job.done():
                print(job.result())
                jobsDone.append(job)
                jobs.pop(ijob)
            else:
                ijob = ijob + 1
        print(f"""{len(jobs):4} jobs remaining""")
        time.sleep(10)
    print("All jobs complete.")
    time.sleep(twait)
    if showio: print(psutil.disk_io_counters())
    if showio: print(psutil.net_io_counters())
    print(f"""Stopping parsl""")
    parsl.dfk().cleanup()     # Needed to stop monitor if we run interactively.
    parsl.clear()
    print(f"""Waiting... for sysmon to finish.""")
    time.sleep(dsam)
    msg = 'Done'
    print("Exiting.")

def main_parsltest():
    if len(sys.argv) > 1 and sys.argv[1] == '-h':
        print(f"Usage: {sys.argv[0]} NJOB NSEC NWRK DSAM MMAX")
        print(f"  NJOB - Number of jobs [0].")
        print(f"  NSEC - Run time for the Nth job is N*NSEC [10.")
        print(f"  NWRK - Number of worker nodes [4].")
        print(f"  DSAM - Sampling interval ins seconds [10].")
        print(f"  MMAX - Memory limit per job in GB [10].")
        return 0
    njob = 0
    sfac = 10.0
    nwrk = 4
    dsam = 10
    mmax = 10
    if len(sys.argv) > 1:
        njob = int(sys.argv[1])
    if len(sys.argv) > 2:
        sfac = float(sys.argv[2])
    if len(sys.argv) > 3:
        nwrk = int(sys.argv[3])
    if len(sys.argv) > 4:
        dsam = int(sys.argv[4])
        if dsam == 0:
            dsam = float(sys.argv[4])
    if len(sys.argv) > 5:
        mmax = int(sys.argv[5])
    if njob > 0: parsltest(njob, sfac, max_workers=nwrk, dsam=dsam, memmax=mmax)

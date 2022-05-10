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
import random

import faulthandler
import signal

def make_config(max_workers =4, dsam =10, sexec='ht'):
    if sexec == 'wq':
      executor = parsl.WorkQueueExecutor(
                   autolabel=True,
                   autocategory=True
                 )
    elif sexec == 'ht':
      executor = parsl.HighThroughputExecutor(
                   label="local_htex",
                   cores_per_worker=1,
                   max_workers=max_workers,
                   address=parsl.addresses.address_by_hostname(),
                 )
    else:
        raise Exception(f"Invalid executor specifier: {sexec}")
    config = parsl.config.Config(
       executors=[ executor ],
       monitoring=parsl.monitoring.monitoring.MonitoringHub(
           hub_address=parsl.addresses.address_by_hostname(),
           hub_port=55055,
           monitoring_debug=False,
           resource_monitoring_interval=dsam,
       ),
       strategy=None
    )
    return config

@parsl.python_app
def myjob(name, trun):
    import time
    print(f"""Running job {name} for {trun:.1f} seconds""")
    time.sleep(trun)
    return f"""Finished job {name}"""

@parsl.bash_app
def mybash(name, trun, memmax):
    return f"""desc-cpuburn {name} {trun} {memmax} 2>&1 >{name}.log; echo Finished job {name}"""

def parsltest(njob =4, tmax =10, memmax =10, clean =True, twait =5, max_workers =4, dsam =1, sexec ='ht'):
    faulthandler.register(signal.SIGUSR2.value)  # So kill -s SIGUSR2 <pid> will show trace.
    myname = 'parsltest'
    print(f"{myname}Welcome to parsltest")
    print(f"{myname}: Parsl version is {parsl.version.VERSION}")
    print(f"{myname}: Desc-sysmon version is {desc.sysmon.__version__}")
    tjob = []
    if njob <= 0:
        print("f{myname}: Running no jobs.")
    elif njob == 1:
        print("f{myname}: Running 1 job for {tmax:.1f} sec.")
        tjob.append(tmax)
    else:
        t0 = 0.5*tmax
        dtjob = (tmax-t0)/(njob-1)
        for ijob in range(njob):
            tjob.append(tmax - ijob*dtjob)
        random.shuffle(tjob)
        print(f"{myname}: Running {njob} jobs for {min(tjob):.1f} - {max(tjob):.1f} sec.")
    print(f"{myname}: Job memory limit is {memmax} GB.")
    print(f"{myname}: Number of workers: {max_workers}.")
    print(f"{myname}: Monitor sampling time: {dsam} seconds.")
    if clean: os.system('./clean')
    #parsl.clear()
    cfg = make_config(max_workers, dsam, sexec)
    msg = desc.sysmon.Notify()
    thr = desc.sysmon.reporter('sysmon.csv', check=msg, dbg=3, thr=True)
    parsl.load(cfg)
    jobs = []
    jobsDone = []
    for ijob in range(njob):
        jobs.append( mybash(f'job{ijob:02}', tjob[ijob], memmax) )
    showio = False
    if showio: print(f"{myname}: Disk I/O: {psutil.disk_io_counters()}")
    if showio: print(f"{myname}: Netw I/O: {psutil.net_io_counters()}")
    while len(jobs):
        ijob = 0
        while ijob < len(jobs):
            job = jobs[ijob]
            if job.done():
                print(f"{myname}: Job result: {job.result()}")
                jobsDone.append(job)
                jobs.pop(ijob)
            else:
                ijob = ijob + 1
        print(f"{myname}: {len(jobs):4} jobs remaining")
        time.sleep(10)
    print(f"{myname}: All jobs complete.")
    time.sleep(twait)
    if showio: print(f"{myname}: Disk I/O: {psutil.disk_io_counters()}")
    if showio: print(f"{myname}: Netw I/O: {psutil.net_io_counters()}")
    print(f"{myname}: Waiting... for sysmon to finish.")
    #time.sleep(dsam)
    msg.set('Done')
    thr.join()
    print(f"{myname}: Exiting.")

def main_parsltest():
    if len(sys.argv) > 1 and sys.argv[1] == '-h':
        print(f"Usage: {sys.argv[0]} NJOB NSEC NWRK DSAM MMAX")
        print(f"  NJOB - Number of jobs [0].")
        print(f"  NSEC - Run time for the Nth job.")
        print(f"  NWRK - Number of worker nodes [4].")
        print(f"  DSAM - Sampling interval in seconds [10].")
        print(f"  MMAX - Memory limit per job in GB [10].")
        print(f"  SEXC - Executor: ht, wq or tp [xx]")
        return 0
    njob = 0
    tmax = 60.0
    nwrk = 4
    dsam = 10
    mmax = 10
    sexc = 'xx'
    if len(sys.argv) > 1:
        njob = int(sys.argv[1])
    if len(sys.argv) > 2:
        tmax = float(sys.argv[2])
    if len(sys.argv) > 3:
        nwrk = int(sys.argv[3])
    if len(sys.argv) > 4:
        dsam = int(sys.argv[4])
        if dsam == 0:
            dsam = float(sys.argv[4])
    if len(sys.argv) > 5:
        mmax = float(sys.argv[5])
    if len(sys.argv) > 6:
        sexc = sys.argv[6]
    if njob > 0: parsltest(njob, tmax, max_workers=nwrk, dsam=dsam, memmax=mmax, sexec=sexc)

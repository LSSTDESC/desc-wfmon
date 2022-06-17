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
    '''
    Build a config for an executor.
      sexec - specifies the executor
        ht = HighThroughput
        wq = WorkQueue
        tp = Threadpool
      max_workers - # workers = target # concurrent tasks
                    For wq, this is the total system memory in MB
      dsam - Monitor sampling interval [sec]
    '''
    if sexec == 'wq':
      executor = parsl.WorkQueueExecutor(
                   worker_options=f"--memory={max_workers}"
                 )
    elif sexec == 'ht':
      executor = parsl.HighThroughputExecutor(
                   label="local_htex",
                   cores_per_worker=1,
                   max_workers=max_workers,
                   address=parsl.addresses.address_by_hostname(),
                 )
    elif sexec == 'tp':
      executor = parsl.ThreadPoolExecutor(
                   max_threads=max_workers,
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

# Bash app which runs for fixed time.
#   stdout and stderr are ouput log file names.
#   parsl_resource_specification is used by WorkQueue
@parsl.bash_app
def mybash_tfix(name, trun, memmax, outdir, stdout, stderr,
                parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
    scom = f"desc-cpuburn {name} {trun} {memmax} 0 10000 1 {outdir}; echo Finished job {name}"
    print(f"mybash_tfix: {scom}")
    return scom

# Bash app which runs a fixed numnber instructions.
# Replace time limt with anestimate for the max nuymber of waveforms
#   stdout and stderr are ouput log file names.
#   parsl_resource_specification is used by WorkQueue
@parsl.bash_app
def mybash_ifix(name, trun, memmax, ngen, outdir, stdout, stderr,
                parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
    nwfPerSec = [730, 400, 300]
    nwfmax = int(trun*nwfPerSec[ngen])
    scom = f"desc-cpuburn {name} {0} {memmax} {nwfmax} 10000 {ngen} {outdir}; echo Finished job {name}"
    print(f"mybash_ifix: {scom}")
    return scom

def parsltest(njob =4, tmax =10, memmax =10, clean =False, twait =5, max_workers =4, dsam =1, sexec ='ht'):
    faulthandler.register(signal.SIGUSR2.value)  # So kill -s SIGUSR2 <pid> will show trace.
    myname = 'parsltest'
    print(f"{myname}: Welcome to parsltest")
    print(f"{myname}: Parsltest version is {desc.parsltest.__version__}")
    print(f"{myname}: Parsltest location is {desc.parsltest.__file__}")
    print(f"{myname}: Parsl version is {parsl.version.VERSION}")
    print(f"{myname}: Parsl location is {parsl.__file__}")
    tjob = []
    res_spec = None
    if njob <= 0:
        print(f"{myname}: Running no jobs.")
    elif njob == 1:
        print(f"{myname}: Running 1 job for {tmax:.1f} sec.")
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
    if sexec == 'wq':
        max_workers = int(1024*memmax*max_workers)
        res_spec = {'cores': 1, 'memory': 1024*int(memmax), 'disk': 1000}
        print(f"{myname}: System memory limit: {max_workers} MB.")
        print(f"{myname}: Resource spec: {res_spec}.")
    print(f"{myname}: Monitor sampling time: {dsam} seconds.")
    print(f"{myname}: Executor: {sexec}.")
    # Resource specs for the WorkQueue executor.
    if clean: os.system('./clean')
    #parsl.clear()
    cfg = make_config(max_workers, dsam, sexec)
    msg = desc.sysmon.Notify()
    thr = desc.sysmon.reporter('sysmon.csv', dt=dsam, check=msg, dbg=3, thr=True)
    parsl.load(cfg)
    jobs = []
    jobsDone = []
    ngen = 1
    pwd = os.getcwd()
    outdir =    f"{pwd}/out"
    logoutdir = f"{pwd}/logo"
    logerrdir = f"{pwd}/loge"
    for dir in [outdir, logoutdir, logerrdir]:
        if not os.path.isdir(dir) and not os.path.islink(dir):
            print(f"{myname}: Creating directory {dir}")
            os.mkdir(dir)
    for ijob in range(njob):
        fout = f"logo/jobout{ijob:02}.log"
        ferr = f"loge/joberr{ijob:02}.log"
        jobs.append( mybash_ifix(f"job{ijob:02}", tjob[ijob], memmax, ngen, outdir,
                     stdout=fout, stderr=ferr,
                     parsl_resource_specification=res_spec) )
    showio = False
    if showio: print(f"{myname}: Disk I/O: {psutil.disk_io_counters()}")
    if showio: print(f"{myname}: Netw I/O: {psutil.net_io_counters()}")
    while len(jobs):
        ijob = 0
        while ijob < len(jobs):
            job = jobs[ijob]
            if job.done():
                #print(f"{myname}: Completed: {job}")
                e = job.exception()
                if e is None:
                    print(f"{myname}: Job result: {job.result()}")
                else:
                    print(e)
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
    # Run user closeout script. E.g. to delete data files.
    os.system('./closeout')
    print(f"{myname}: Exiting.")

def main_parsltest():
    if len(sys.argv) > 1 and sys.argv[1] == '-v':
        print(f"{desc.parsltest.__version__}")
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == '-h':
        print(f"Usage: {sys.argv[0]} NJOB NSEC NWRK DSAM MMAX")
        print(f"  NJOB - Number of jobs [0].")
        print(f"  NSEC - Run time for the Nth job.")
        print(f"  NWRK - Number of worker nodes [4].")
        print(f"  DSAM - Sampling interval in seconds [10].")
        print(f"  MMAX - Memory limit per job in GB [10].")
        print(f"  SEXC - Executor: ht, wq or tp [xx]")
        print(f"parsltest version is {desc.parsltest.__version__}")
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

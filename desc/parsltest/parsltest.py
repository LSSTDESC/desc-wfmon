# parsltest.py
#
# David Adams
# January 2022

import parsl
import time
import psutil
import sys
import os
import stat
import desc.sysmon
import random

import faulthandler
import signal

# Return the parsl WorQue fundtion dir.
# Making this local can improve the launch rate.
def get_function_dir():
  nam = f"/tmp/{os.environ.get('USER')}/parsl"       # for 1.3.0-dev+desc-2022.09.26b only e.g. use '/tmp'
  if not os.path.exists(nam):
      os.makedirs(nam)
  return nam

doParslLogging = True

doParslTracing = 0     # 0 for no trace, 1 at end of job, 2 after all tasks are created

def make_config(nwrk, node_memory, dsam =10, sexec='ht', nnod=0):
    '''
    Build a config for an executor.
      sexec - specifies the executor
        ht = HighThroughput
        wq = WorkQueue
        ww = WorkQueue waiting for workers (start with work_queue_worker <host> 9123)
        tp = Threadpool
      nwrk = # workers = target # concurrent tasks/node for ht and tp
      node_memory = memory/node for wq [MB]
      dsam - Monitor sampling interval [sec]
    '''
    if sexec == 'wq':
        executor = parsl.WorkQueueExecutor(
                       worker_options=f"--memory={node_memory}",
                       function_dir=get_function_dir(),
                   )
    elif sexec == 'ww':
        executor = parsl.WorkQueueExecutor(
                       worker_options=f"--memory={node_memory}",
                       function_dir=get_function_dir(),
                       provider = parsl.providers.LocalProvider(init_blocks=0, min_blocks=0, max_blocks=0)
                   )
    elif sexec == 'ht':
      executor = parsl.HighThroughputExecutor(
                   label="local_htex",
                   cores_per_worker=1,
                   max_workers=nwrk,
                   address=parsl.addresses.address_by_hostname(),
                 )
    elif sexec == 'tp':
      executor = parsl.ThreadPoolExecutor(
                   max_threads=nwrk,
                 )
    else:
        raise Exception(f"Invalid executor specifier: {sexec}")
    if nnod > 1:
        executor.provider.launcher = parsl.launchers.SrunLauncher(overrides='-K0 -k --slurmd-debug=verbose')
        executor.provider.nodes_per_block = nnod
    enabled = dsam > 0
    interval = dsam if enabled else 999
    config = parsl.config.Config(
       initialize_logging=doParslLogging,
       executors=[ executor ],
       monitoring=parsl.monitoring.monitoring.MonitoringHub(
           hub_address=parsl.addresses.address_by_hostname(),
           hub_port=55055,
           monitoring_debug=False,
           resource_monitoring_enabled=enabled,
           resource_monitoring_interval=interval,
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

# Bash app which sleeps.
#   stdout and stderr are ouput log file names.
#   parsl_resource_specification is used by WorkQueue
@parsl.bash_app
def mybash_sleep(name, trun, mtsk, outdir, ngen, stdout, stderr,
                parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
    scom = f"time sleep {trun}; echo Finished job {name} on $(hostname) at $(date)"
    print(f"mybash_sleep: {scom}")
    return scom

# Bash app which runs cpuburn for a fixed time.
#   stdout and stderr are ouput log file names.
#   parsl_resource_specification is used by WorkQueue
@parsl.bash_app
def mybash_tfix(name, trun, mtsk, ngen, outdir, stdout, stderr,
                parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
    scom = f"desc-cpuburn {name} {trun} {mtsk} 0 10000 1 {outdir}; echo Finished job {name}"
    print(f"mybash_tfix: {scom}")
    return scom

# Bash app which runs cpuburn for a fixed number of instructions.
# Replace time limt with anestimate for the max nuymber of waveforms
#   stdout and stderr are ouput log file names.
#   parsl_resource_specification is used by WorkQueue
@parsl.bash_app
def mybash_ifix(name, trun, mtsk, ngen, outdir, stdout, stderr,
                parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
    nwfPerSec = [730, 400, 300]
    nwfmax = int(trun*nwfPerSec[ngen])
    scom = f"desc-cpuburn {name} {0} {mtsk} {nwfmax} 10000 {ngen} {outdir}; echo Finished job {name}"
    print(f"mybash_ifix: {scom}")
    return scom

def parsltest(jobdesc, ttsk, mtsk, ntsk, sexec, nwrk, clean =False, twait =5, dsam =1, nnode=0):
    faulthandler.register(signal.SIGUSR2.value)  # So kill -s SIGUSR2 <pid> will show trace.
    myname = 'parsltest'
    print(f"{myname}: Welcome to parsltest")
    print(f"{myname}: Parsltest version is {desc.parsltest.__version__}")
    print(f"{myname}: Parsltest location is {desc.parsltest.__file__}")
    print(f"{myname}: Parsl version is {parsl.version.VERSION}")
    print(f"{myname}: Parsl location is {parsl.__file__}")
    global doParslTracing
    if doParslTracing:
        print(f"{myname}: WARNING: Enabling parsl tracing.")
        parsl.trace.trace_by_dict=True
    tjob = []
    res_spec = None
    if jobdesc in ['sleep']:
        jobtype = jobdesc
    elif jobdesc in ['ifixw', 'ifixn']:
        jobtype = 'ifix'
    else:
        print(f"{nyname}: ERROR: Invalid job description: {jobdesc}")
        return 1
    if ntsk <= 0:
        print(f"{myname}: Running no jobs.")
    elif ntsk == 1:
        print(f"{myname}: Running 1 job for {ttsk:.1f} sec.")
        tjob.append(ttsk)
    else:
        t0 = 0.5*ttsk
        dtjob = (ttsk-t0)/(ntsk-1)
        for ijob in range(ntsk):
            tjob.append(ttsk - ijob*dtjob)
        random.shuffle(tjob)
        print(f"{myname}: Running {ntsk} jobs for {min(tjob):.1f} - {max(tjob):.1f} sec.")
    print(f"{myname}: Job type is {jobtype}.")
    print(f"{myname}: Job memory limit is {mtsk} GB.")
    print(f"{myname}: Number of workers: {nwrk}.")
    node_memory = 100.0
    if sexec == 'wq':
        node_memory = int(1024*mtsk*nwrk)
        res_spec = {'cores': 1, 'memory': 1024*int(mtsk), 'disk': 1000}
        res_spec['running_time_min'] = ttsk*1.1
        print(f"{myname}: System memory limit: {node_memory} MB.")
        print(f"{myname}: Resource spec: {res_spec}.")
    print(f"{myname}: Monitor sampling time: {dsam} seconds.")
    print(f"{myname}: Executor: {sexec}.")
    print(f"{myname}: Number of nodes: {nnode}.")
    # Resource specs for the WorkQueue executor.
    if clean: os.system('./clean')
    #parsl.clear()
    cfg = make_config(nwrk, node_memory, dsam, sexec, nnode)
    msg = desc.sysmon.Notify()
    dsamsys = dsam if dsam > 0 else 5
    thr = desc.sysmon.reporter('sysmon.csv', dt=dsamsys, check=msg, dbg=3, thr=True)
    parsl.load(cfg)
    jobs = []
    jobsDone = []
    ngen = 1
    pwd = os.getcwd()
    outdir =    f"{pwd}/out"
    if jobdesc in ['ifixn']:
        outdir = '/dev/null'
    logoutdir = f"{pwd}/logo"
    logerrdir = f"{pwd}/loge"
    for dir in [outdir, logoutdir, logerrdir]:
        if dir != '/dev/null' and not os.path.isdir(dir) and not os.path.islink(dir):
            print(f"{myname}: Creating directory {dir}")
            os.mkdir(dir)
    for ijob in range(ntsk):
        fout = f"logo/jobout{ijob:04}.log"
        ferr = f"loge/joberr{ijob:04}.log"
        sjob  = f"mybash_{jobtype}('job{ijob:04}', tjob[ijob], mtsk, ngen, outdir, "
        sjob += f"stdout=fout, stderr=ferr, parsl_resource_specification=res_spec)"
        print(f"Creating: {sjob}")
        print(f"  fout: {fout}")
        print(f"  ferr: {ferr}")
        jobs.append(eval(sjob))
        assert(jobs[-1] is not None)
        #print(f"Created {jobs[-1]}")
    showio = False
    if showio: print(f"{myname}: Disk I/O: {psutil.disk_io_counters()}")
    if showio: print(f"{myname}: Netw I/O: {psutil.net_io_counters()}")
    if doParslTracing > 1:
        print(f"Writing parsl stats.")
        parsl.trace.output_event_stats()
        doParslTracing = False
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
    if doParslTracing:
        print(f"Writing parsl stats.")
        parsl.trace.output_event_stats()
    # Run user closeout script. E.g. to delete data files.
    os.system('./closeout')
    print(f"{myname}: Exiting.")

def parsltest_from_string(sargs):
    '''
    Create a parsl test from a string made up for dash-separated fields:
    ifixw, ifixn or sleep - Job type
    ttsk - Average task run time [s]
    mtsk - Task memory estimate [GB]
    wq, ww, ht, tp - Executor
    ntsk - Total # tasks to run
    nwrk - # concurrent tasks for ht or tp
    '''
    jtyp = None
    ttsk = None
    mtsk = 1
    ntsk = None
    sexe = None
    nwrk = None
    dsam = 5
    nnod = 0
    emsgs = []
    for sarg in sargs.split('-'):
        if sarg in ['sleep', 'ifixw', 'ifixn']:
            jtyp = sarg
        elif sarg[0:4] == 'ttsk':
            ttsk = int(sarg[4:])
        elif sarg[0:4] == 'mtsk':
            mtsk = int(sarg[4:])
        elif sarg[0:4] == 'ntsk':
            ntsk = int(sarg[4:])
        elif sarg in ['wq', 'ww', 'ht', 'tp']:
            sexe = sarg
        elif sarg[0:4] == 'nwrk':
            nwrk = int(sarg[4:])
        elif sarg[0:4] == 'dsam':
            dsam = int(sarg[4:])
        elif sarg[0:4] == 'nnod':
            nnod = int(sarg[4:])
        else:
            emsgs += [f"Invalid field: {sarg}"]
    if jtyp is None: emsgs += ['Job type must be provided']
    if ttsk is None: emsgs += ['Job run time must be provided']
    if ntsk is None: emsgs += ['Number of tasks must be provided']
    if sexe is None: emsgs += ['Executor must be provided']
    if nwrk is None: emsgs += ['Number of workers/node must be provided']
    if len(emsgs):
        myname = 'parsltest'
        for emsg in emsgs:
            print(f"{myname}: ERROR: {emsg}")
        return 0
    if ntsk > 0: parsltest(jobdesc=jtyp, ttsk=ttsk, mtsk=mtsk, ntsk=ntsk, sexec=sexe, nwrk=nwrk, dsam=dsam, nnode=nnod)

def ldj_create_parsltest(sargs, myname):
    '''Create the submit file for a label-driven job'''
    fnam = 'submit'
    if os.path.exists(fnam):
        print(f"{myname}: ERROR: file exists: {fnam}")
        return 1
    fout = open(fnam, "w")
    fout.write(f"desc-wfmon-parsltest {sargs}\n")
    fout.close()
    fst = os.stat(fnam)
    os.chmod(fnam, fst.st_mode | stat.S_IEXEC)

def main_parsltest():
    myname = os.path.basename(sys.argv[0])
    narg = len(sys.argv)
    dohelp = narg != 2
    helpstat = 1
    if not dohelp:
        sargs = sys.argv[1]
        if sargs == '-h':
            dohelp = True
            helpstat = 0
    if dohelp:
        print(f"Usage: {myname} OPT1-OPT2-... where options OPTi include")
        print(f"  sleep, ifixw or ifixn: Job type")
        print(f"  ttskTTT: Nominal task run time is TTT sec")
        print(f"  mtskMMM: Nominal task memory size time is MMM GB [1]")
        print(f"  ntskNNN: Nominal number of tasks is NNN")
        print(f"  wq, ww, ht or tp: Parsl executor")
        print(f"  nwrkNNN: number of workers/node")
        print(f"  dsamTTT: monitor sampling period in sec [5]")
        print(f"  nnodNNN: Number of nodes [0]")
        return helpstat
    if myname == 'desc-wfmon-parsltest':
        return parsltest_from_string(sargs)
    elif myname == 'ldj-create-parsltest':
        return ldj_create_parsltest(sargs, myname)
    else:
        print(f"ERROR: unrecognized command mapping: {myname}")
        return 1

def main_parsltest_old():
    myname = os.path.basename(sys.argv[0])
    if len(sys.argv) > 1 and sys.argv[1] == '-v':
        print(f"{desc.parsltest.__version__}")
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == '-h':
        print(f"Usage: {myname} JTYP NTSK TTSK NWRK DSAM MTSK SEXC NNOD")
        print(f"  JTYP - Job type (ifixw, ifixn, sleep, ...)")
        print(f"  TTSK - Run time for the Nth job.")
        print(f"  MTSK - Memory limit per job in GB [10].")
        print(f"  NTSK - Number of tasks [0].")
        print(f"  SEXC - Executor: ht, wq or tp [xx]")
        print(f"  NWRK - Number of worker nodes for ht or tp [4].")
        print(f"  DSAM - Sampling interval in seconds [10].")
        print(f"  NNOD - # worker nodes (0 is local submission)")
        print(f"parsltest version is {desc.parsltest.__version__}")
        return 0
    ntsk = 0
    ttsk = 60.0
    nwrk = 4
    dsam = 10
    mtsk = 10
    sexe = 'xx'
    nnod = 0
    if len(sys.argv) > 1:
        jtyp = sys.argv[1]
    if len(sys.argv) > 2:
        ntsk = int(sys.argv[2])
    if len(sys.argv) > 3:
        ttsk = float(sys.argv[3])
    if len(sys.argv) > 4:
        nwrk = int(sys.argv[4])
    if len(sys.argv) > 5:
        dsam = int(sys.argv[5])
        #if dsam == 0:
        #    dsam = float(sys.argv[5])
    if len(sys.argv) > 6:
        mtsk = float(sys.argv[6])
    if len(sys.argv) > 7:
        sexe = sys.argv[7]
    if len(sys.argv) > 8:
        nnod = int(sys.argv[8])
    if ntsk > 0: parsltest(jobtype=jtyp, ttsk=ttsk, mtsk=mtsk, ntsk=ntsk, sexec=sexe, nwrk=nwrk, dsam=dsam, nnode=nnod)

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

# Return the parsl WorQueue function dir.
# Making this local can improve the launch rate.
def get_function_dir():
  uid = os.getuid()
  unam = pwd.getpwuid(uid).pw_name
  if unam is None: unam = 'unknown'
  nam = f"/tmp/{unam}/parsl"       # for 1.3.0-dev+desc-2022.09.26b only e.g. use '/tmp'
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
        ww = WorkQueue waiting for workers (start with work_queue_worker <host> <port>)
        tp = Threadpool
      nwrk = # workers = target # concurrent tasks/node for ht and tp
      node_memory = memory/node for wq [MB]
      dsam - Monitor sampling interval [sec]
    '''
    wqopts = ''
    worker_port = 0
    if node_memory > 0:
        wqopts += f" --memory={node_memory}"
    if nwrk > 0:
        wqopts += f" --cores={nwrk}"
    if sexec == 'wq':
        executor = parsl.WorkQueueExecutor(
                       worker_options=wqopts,
                       port=worker_port,
                       #function_dir=get_function_dir(),
                   )
    elif sexec == 'ww':
        executor = parsl.WorkQueueExecutor(
                       worker_options=wqopts,
                       port=worker_port,
                       #function_dir=get_function_dir(),
                       provider = parsl.providers.LocalProvider(init_blocks=0, min_blocks=0, max_blocks=0),
                       radio_mode = 'results',
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
           #hub_port=55055,
           monitoring_debug=False,
           resource_monitoring_enabled=enabled,
           resource_monitoring_interval=interval,
       ),
       strategy=None
    )
    return config

@parsl.python_app
def mytsk(name, trun):
    import time
    print(f"""Running task {name} for {trun:.1f} seconds""")
    time.sleep(trun)
    return f"""Finished task {name}"""

# Bash app which sleeps.
#   stdout and stderr are ouput log file names.
#   parsl_resource_specification is used by WorkQueue
@parsl.bash_app
def mybash_sleep(name, trun, mtsk, outdir, ngen, stdout, stderr,
                parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
    scom = f"echo Started task {name} on $(hostname) at $(date); time sleep {trun}; echo Finished task {name} on $(hostname) at $(date)"
    print(f"mybash_sleep: {scom}")
    return scom

# Bash app which runs cpuburn for a fixed time.
#   stdout and stderr are ouput log file names.
#   parsl_resource_specification is used by WorkQueue
@parsl.bash_app
def mybash_tfix(name, trun, mtsk, ngen, outdir, stdout, stderr,
                parsl_resource_specification={'cores': 1, 'memory': 1000, 'disk': 1000}):
    scom = f"desc-cpuburn {name} {trun} {mtsk} 0 10000 1 '{outdir}'; echo Finished task {name}"
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
    scom = f"desc-cpuburn {name} {0} {mtsk} {nwfmax} 10000 {ngen} '{outdir}'; echo Finished task {name}"
    print(f"mybash_ifix: {scom}")
    return scom

def parsltest(tskdesc, ttsk, mtsk, ntsk, sexec, nwrk, clean =False, twait =5, dsam =1, nnode=0):
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
    ttsks = []
    res_spec = None
    if tskdesc in ['sleep']:
        tsktype = tskdesc
    elif tskdesc in ['ifixw', 'ifixn', 'ifix0']:
        tsktype = 'ifix'
    else:
        print(f"{nyname}: ERROR: Invalid task description: {tskdesc}")
        return 1
    if ntsk <= 0:
        print(f"{myname}: Running no tasks.")
    elif ntsk == 1:
        print(f"{myname}: Running 1 task for {ttsk:.1f} sec.")
        ttsks.append(ttsk)
    else:
        t0 = 0.5*ttsk
        dttsk = (ttsk-t0)/(ntsk-1)
        for itsk in range(ntsk):
            ttsks.append(ttsk - itsk*dttsk)
        random.shuffle(ttsks)
        print(f"{myname}: Running {ntsk} tsaks for {min(ttsks):.1f} - {max(ttsks):.1f} sec.")
    print(f"{myname}: Task type is {tsktype}.")
    print(f"{myname}: Task memory size is {mtsk} GB.")
    print(f"{myname}: Number of workers: {nwrk}.")
    use_nwrk_memory = False
    node_memory = 0.0
    if sexec in ['wq', 'ww']:
        if use_nwrk_memory:
            node_memory = int(1024*mtsk*nwrk)
        res_spec = {'cores': 1, 'memory': 1024*int(mtsk), 'disk': 1000}
        res_spec['running_time_min'] = ttsk*1.1
        print(f"{myname}: System memory limit: {node_memory} MB.")
        print(f"{myname}: Task resource spec: {res_spec}.")
    else:
        print(f"({myname}: Not setting task resource specs for exector {sexec}.")
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
    tsks = []
    tsksDone = []
    ngen = 1
    pwd = os.getcwd()
    outdir =    f"{pwd}/out"
    if tskdesc in ['ifixn']:
        outdir = '/dev/null'
    if tskdesc in ['ifix0']:
        outdir = ''
    logoutdir = f"{pwd}/logo"
    logerrdir = f"{pwd}/loge"
    for dir in [outdir, logoutdir, logerrdir]:
        if len(dir) and dir != '/dev/null' and not os.path.isdir(dir) and not os.path.islink(dir):
            print(f"{myname}: Creating directory {dir}")
            os.mkdir(dir)
    for itsk in range(ntsk):
        fout = f"logo/taskout{itsk:04}.log"
        ferr = f"loge/taskerr{itsk:04}.log"
        stsk  = f"mybash_{tsktype}('tsk{itsk:04}', ttsks[itsk], mtsk, ngen, outdir, "
        stsk += f"stdout=fout, stderr=ferr, parsl_resource_specification=res_spec)"
        print(f"Creating: {stsk}")
        print(f"  fout: {fout}")
        print(f"  ferr: {ferr}")
        tsks.append(eval(stsk))
        assert(tsks[-1] is not None)
        #print(f"Created {tsks[-1]}")
    showio = False
    if showio: print(f"{myname}: Disk I/O: {psutil.disk_io_counters()}")
    if showio: print(f"{myname}: Netw I/O: {psutil.net_io_counters()}")
    if doParslTracing > 1:
        print(f"Writing parsl stats.")
        parsl.trace.output_event_stats()
        doParslTracing = False
    donetaskcounts = {}
    while len(tsks):
        itsk = 0
        nrun = 0
        taskcounts = {}
        while itsk < len(tsks):
            tsk = tsks[itsk]
            sstat = tsk.task_status()
            if tsk.done():
                if sstat not in donetaskcounts:
                    donetaskcounts[sstat] = 0
                donetaskcounts[sstat] += 1
                e = tsk.exception()
                if e is None:
                    print(f"{myname}: Job result: {tsk.result()}")
                else:
                    print(e)
                tsksDone.append(tsk)
                tsks.pop(itsk)
            else:
                if sstat not in taskcounts:
                    taskcounts[sstat] = 0
                taskcounts[sstat] += 1
                itsk += 1
        line = f"{len(tsks):4}/{ntsk} tasks remaining"
        sep = ':'
        for sstat in donetaskcounts:
            line += f"{sep} {donetaskcounts[sstat]:4} {sstat}"
            sep = ','
        if sep == ',': sep = ';'
        for sstat in taskcounts:
            line += f"{sep} {taskcounts[sstat]:4} {sstat}"
            sep = ','
        print(f"{myname}: {line}")
        sout = open('current-status.txt', 'w')
        sout.write(line + '\n')
        sout.close()
        time.sleep(10)
    line = f"All {ntsk} tasks complete"
    sep = ':'
    for sstat in donetaskcounts:
        line += f"{sep} {donetaskcounts[sstat]:4} {sstat}"
        sep = ','
    print(f"{myname}: {line}")
    sout = open('current-status.txt', 'w')
    sout.write(line + '\n')
    sout.close()
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
    ifixw, ifixn, ifix0 or sleep - Job type
    ttsk - Average task run time [s]
    mtsk - Task memory usage [1 GB]
    wq, ww, ht, tp - Executor
    ntsk - Total # tasks to run
    nwrk - # concurrent tasks for ht or tp
    dsam - 
    '''
    jtyp = None
    ttsk = None
    mtsk = 1
    ntsk = None
    sexe = None
    nwrk = 0
    dsam = 5
    nnod = 0
    emsgs = []
    for sarg in sargs.split('-'):
        if sarg in ['sleep', 'ifixw', 'ifixn', 'ifix0']:
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
    if nwrk == 0 and sexe in ['wq', 'ht', 'tp']: emsgs += ['Number of workers/node must be provided']
    if len(emsgs):
        myname = 'parsltest'
        for emsg in emsgs:
            print(f"{myname}: ERROR: {emsg}")
        return 0
    if ntsk > 0: parsltest(tskdesc=jtyp, ttsk=ttsk, mtsk=mtsk, ntsk=ntsk, sexec=sexe, nwrk=nwrk, dsam=dsam, nnode=nnod)

def ldj_create_parsltest(sargs, myname):
    '''Create the submit file for a label-driven tsk'''
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
        print(f"  A task type:")
        print(f"    sleep - Sleep for a specified time")
        print(f"    ifixw - Run cpuburn a specified time, writing output")
        print("             when task memory is reached.")
        print(f"    ifixn - Same except writes are to /dev/null.")
        print(f"    ifix0 - Same without writing anything out.")
        print(f"  ttskTTT: Nominal task run time is TTT sec")
        print(f"  mtskMMM: Nominal task memory size time is MMM GB [1]")
        print(f"  ntskNNN: Nominal number of tasks is NNN")
        print(f"  wq, ww, ht or tp: Parsl executor")
        print(f"  nwrkNNN: number of workers/node")
        print(f"  dsamTTT: monitor sampling period in sec [5]")
        print(f"  nnodNNN: Number of nodes [0]")
        return helpstat
    if sargs == '-v':
        print(f"{desc.parsltest.__version__}")
        return 0
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
        print(f"  JTYP - Job type (ifixw, ifixn, ifix0, sleep, ...)")
        print(f"  TTSK - Run time for the Nth task.")
        print(f"  MTSK - Task memory size in GB [10].")
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
    mtsk = 1
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
    if ntsk > 0: parsltest(tskdesc=jtyp, ttsk=ttsk, mtsk=mtsk, ntsk=ntsk, sexec=sexe, nwrk=nwrk, dsam=dsam, nnode=nnod)

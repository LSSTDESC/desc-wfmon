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

# Sampling time [sec]
dsam = 5

def load_config(max_workers =4):
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
           resource_monitoring_interval=10,
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
    return f"""python cpuburn.py {name} {trun} {memmax} 2>&1 >{name}.log; echo Finished job {name}"""

def parsltest(njob =4, sfac =10, memmax =10, clean =True, twait =5, max_workers =4):
    print('Welcome to parsltest')
    print(f"""Parsl version is {parsl.version.VERSION}""")
    print(f"""Desc-sysmon version is {desc.sysmon.__version__}""")
    msg = desc.sysmon.Notify()
    #thr = desc.sysmon.reporter('out/sysmon.csv', check=msg, dbg=3, thr=True)
    print(f"""Running {njob} jobs for {sfac}, {2*sfac}, ..., {njob*sfac} seconds.""")
    if clean: os.system('./clean')
    #parsl.clear()
    load_config(max_workers)
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
    time.sleep(dsam);
    msg.set('Done')
    print("Exiting.")

def main_parsltest():
    if len(sys.argv) > 1 and sys.argv[1] == '-h':
        print(f"Usage: {sys.argv[0]} NJOB NSEC")
        print(f"  NJOB - Number of jobs.")
        print(f"  NSEC - Run time for the Nth job is N*NSEC")
        return 0
    sfac = 10.0
    if len(sys.argv) > 1:
        njob = int(sys.argv[1])
    if len(sys.argv) > 2:
        sfac = float(sys.argv[2])
    if njob: parsltest(njob, sfac)

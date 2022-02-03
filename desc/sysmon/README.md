# desc.sysmon -- THIS PAGE IS UNDER DEVELOPMENT

This python module provides for system-level monitoring of CPU and memory usage
and disk and network I/O.
It inclues a reporter to collect data at regular intervals and
(soon) tools to examine and plot that data.

## Reporter.

The reporter uses [psutil](https://pypi.org/project/psutil) to collect data
on system performance at regular intervals. The following data are recorded:

            time - Unix time (including fraction) in seconds
       cpu_count
    cpu_percent
    cpu_user
    cpu_system
    cpu_idle
    cpu_iowait
    cpu_time
    mem_total
    mem_available
    mem_swapfree
    dio_readsize
    dio_writesize
    nio_readsize
    nio_writesize
    
The reporter may be called from python, e.g.
<pre>
>>> import desc.sysmon
>>> desc.sysmon.reporter(fnam='sysmon.csv', dt=5)
</pre>
or started from a shell command line
<pre>
> desc-sysmon-reporter "fnam='sysmon.dat';dt=5"
</pre>
with the following fields as arguments:
    
         fnam - Output file name ['sysmon.csv'].
           dt - Polling time interval in seconds [10].
       subcom - If not empty, then subcom is run as a subprocess
                and polling ceases when that process exits ['']. 
        check - If not None polling ceases when check() returns anything except None [None].
      timeout - If nonzero, polling ceases after timeout seconds [0].
          dbg - Log message level: 0=none, 1=minimal, 2=config, 3=every sample.
          thr - If true, reporter is run in a thread and this returns immediately [False]
          log - If non-blank, logging is to this file. Blank means stdout. ['']
          
Any of the arguments may be included or omitted with the values shown in backets used
in the latter case. The defaults may be changed in class Params, e.g.

    >>> desc.sysmon.Params.dbg = 3
    
By default, the python function will not return until the reporter finishes polling.
Set *thr* to True to run the reporter in a separate thread and return control immediately.
    
If *subcom* is not blank, then it will be executed as a shell command after the first sample
is recorded and the reporter will exit one sample after the command exits.

In the [../parsltest/README.md](parsl test) provided here the command is used



List tables and schema in ./monitoring.db:

    >>> import desc.wfmon
    >>> dbr = desc.wfmon.MonDbReader()
    >>> dbr.tables(2)
    
This is after "fixing" the tables. Add fix=False to see the raw tables.

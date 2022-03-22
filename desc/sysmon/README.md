# desc.sysmon

This python module provides for system-level monitoring of CPU and memory usage
and disk and network I/O.
It includes a reporter to collect data at regular intervals.
The output is a csv file which is easily read into a dataframe (*pandas.read_csv*).
Examples making plots using this data and that from the parsl process monitor can
be found in the [monexp notebook](../../ipynb/monexp.ipynb).

## Reporter

The fundtion *reporter* uses [psutil](https://pypi.org/project/psutil) to collect data
on system performance at regular intervals. The following data are recorded.
Unle

             time - Unix time (including fraction) in seconds
        cpu_count - Numnber of logical CPUs.
      cpu_percent - Percentage of CPU being used (max cpu_count) over interval
         cpu_user - Time spent in user mode
       cpu_system - Time spent in system mode
         cpu_idle - Time spent idle
       cpu_iowait - Time spent waiting for I/O (not included in idle)
         cpu_time - Total CPU time (expect cpu_count*time)
        mem_total - Total physicasl memory
    mem_available - Available physical memory.
     mem_swapfree - Available swap memory.
     dio_readsize - Disk read size.
    dio_writesize - Disk write size.
     nio_readsize - Network read size.
    nio_writesize - Network write size.
    
For more information on thesse see https://psutil.readthedocs.io/en/latest.
Times are seconds since the preceding sample is in seconds.
Memory is GB (2^30 byte) at the time of sampling.
Disk and network I/O are the number of GB since the last sampling.
    
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
in the latter case. The defaults are specified in class Params and may be changed there
instead of providing arguments, e.g.

    >>> desc.sysmon.Params.dbg = 3
    
By default, the python function will not return until the reporter finishes polling.
Set *thr* to True to run the reporter in a separate thread and return control immediately.
    
If *subcom* is not blank, then it will be executed as a shell command after the first sample
is recorded and sampling continues until the command exits.
The reporter can be stopped with *timeout*, providing a switch function *check* (e.g. desc.sysmon.Notify),
or by sending SIGTERM to the reporter process.
In all cases, the reporter does one additional sampling before exiting.

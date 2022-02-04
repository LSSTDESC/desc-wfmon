# desc.parsltest

This module provides a function to run a test parsl workflow with a user-specified number of
jobs that are CPU, memory and I/O intensive.

The function can be invoked from python:

    >>> import desc.parsltest
    >>> parlstest.parsltest(NJOB, NSEC)
    
or as a shell command:

    > desc-wfmon-parsltest NJOB NSEC
    
In both cased NJOB jos are launched and the nth job gnerates random numbers for n\*NSEC seconds
and then wites the result to a file. Process and system monitoring data are recorded in
runinfo/monitoring.db and runinfo/sysmon.csv whech may be explored using the tools defined
in [desc.wfmon](/desc/wfmon/README.md) and [desc.sysmon](/desc/sysmon/README.md), respectively.

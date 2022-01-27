# desc.sysmon
System-level monitoring of CPU and memory usage and disk and network I/O.
The module inclues a reporter to collect data at regular intervals and
(soon) tools to examine and plot the data.

## Reporter.

The reporter may be called from python, e.g.
<pre>
>>> :X


List tables and schema in ./monitoring.db:

    >>> import desc.wfmon
    >>> dbr = desc.wfmon.MonDbReader()
    >>> dbr.tables(2)
    
This is after "fixing" the tables. Add fix=False to see the raw tables.

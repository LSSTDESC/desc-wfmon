# desc.wfmon
Module for reading and plotting information in the parsl monitoring database and logs.

## Using the package

The following classes are provided. To try out any of the indicated notebooks, copy them to a directory where a DESC parsl job has been run.

### *MonDbReader*
The class *MonDbReader* reads parsl processing monitor DB data into pandas dataframes. To list the tables and schema in ./monitoring.db:

    >>> import desc.wfmon
    >>> dbr = desc.wfmon.MonDbReader()
    >>> dbr.tables(2)
    
This is after "fixing" the tables. Add fix=False to see the raw tables. This code and resulting oputput may be seen in the [montab notebook](../../ipynb/montab.ipynb) and others.  

The class also generates a summary table *procsum* that sums contributions for all active processes in the *try* table. Examples of use the class (and the corresponding one for the system monitor) to make monitoring plots can be found in the [monexp notebook](../../ipynb/monexp.ipynb).

### FunctionData
Class *FunctionData* reads the function_data logs which record the times at which task enter states at a finer granularity than that provided by the parsl monitoring DB. Examples of use can be found in the [fundata notebook](../../ipynb/fundata.ipynb).

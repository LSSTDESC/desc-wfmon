# desc.wfmon
Module for reading and plotting information in the parsl monidtoring database.

## Using the package

List tables and schema in ./monitoring.db:

    >>> import desc.wfmon
    >>> dbr = desc.wfmon.MonDbReader()
    >>> dbr.tables(2)
    
This is after "fixing" the tables. Add fix=False to see the raw tables.

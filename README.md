# desc-wfmon
Python-based workflow monitor for DESC image processing

## Installation
    > git clone https://github.com/LSSTDESC/desc-wfmon.git
    > cd desc-wfmon
    > pip install .

## Modules

List tables and schema in ./monitoring.db:

    >>> import desc.wfmon
    >>> dbr = desc.wfmon.MonDbReader()
    >>> dbr.tables(2)
    
This is after "fixing" the tables. Add fix=False to see the raw tables.

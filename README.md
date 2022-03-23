# desc-wfmon

David Adams  
February 2022  

This python-based DESC workflow monitoring package provides provides tools for examining
the process monitor DB produced by [parsl](https://parsl-project.org).
It also provides a reporter for collecting system-level data aout CPU, memory and I/O
that can be run standalone or integrated with parsl or any other workflow system.
Tools to visualize the process data and examine and visualize the sytem-level
data will be added soon.

## Installation
    > git clone https://github.com/LSSTDESC/desc-wfmon.git
    > cd desc-wfmon
    > pip install .

## Modules

The following modules are included:

- [desc.wfmon](desc/wfmon/README.md): Tools for examining and visualizing data in the parsl process monitoring DB and logs.
- [desc.sysmon](desc/sysmon/README.md): Reporter for recording system-level data and tools for examing and visualizing that data.
- [desc.parsltest](desc/parsltest/README.md): Job definitions and framework for testing parsl and monitoring.
- [desc.local](desc/local/README.md): Tools to aid in local python package installation and access.

Follow the above links or use python help, e.g. help(desc.wfmon) for more information about each module.

## Notebooks

Example python notebooks can be found in [ipynb](ipynb). Copy those to a directory and point and load them in a Jupyter server such as [https://jupyter.nersc.gov].

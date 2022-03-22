# desc.local

David Adams  
February 2022

It is often desireable to add a small number of python packages to an existing python environment which is locked or not writeable. The module desc.local provides a convention and a couple functions for installing and accessing the package modules in a local directory. The convention is to to install the modules in ./install/$LSST_CONDA_ENV_NAME where the env variable is the name LSST assignes to the base environment. The following functions are provided:
* desc.local.install_dir(...) returns the path to the installation directory creating it if it does not already exist. There is a option to first remove the existing installation.
* desc.local.install(...) uses pip to install a package from a specified location. Dependent packages are not installed automatically.

# desc-wfmon
Workflow monitor for DESC image processing

##Installation
> git clone https://github.com/LSSTDESC/desc-wfmon.git
> cd desc-wfmon
> pip install .

## List tables in monitoring ./monitoring.db
import desc.wfmon
dbr = desc.wfmon.MonDbReader(fix=False)
dbr.tables(2)

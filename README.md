# SQL_ETL-using-incremental
This repository contains SQL scripts that creates both OLTP and OLAP files, and perform ETL process from OLTP to OLAP using incremental loading technique.

- 01_StarterScript Create the OLTP DB file is SQL script that creates OLTP database.
- 02_SetupScript Create the DW DB file is SQL script which creates OLAP database.
- ETL_incremental file is SQL script which synchronize OLTP and OLAP database using incremental loading technique including both synchronization and merge. After synchronization, OLAP database will reflect exact same data from OLTP database.

Once the procedures are successfully executed, OLAP database will be used for reporting purposes.

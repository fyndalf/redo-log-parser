# redo-log-parser

![Scala CI](https://github.com/fyndalf/redo-log-parser/workflows/Scala%20CI/badge.svg?branch=master)

Extracts .xes event logs out of database redo logs.

Run by calling
```bash
$ sbt run <path to log>
```

or

```bash
$ sbt run <path to log> --verbose
```

for more information in the output.

The redo log file should be a plain text file. 
The required format is described in the .pdf file provided in this repository.

The tool will ask for a root class, which can be entered using the terminal.
The root class must be one of the discovered tables. 
For this purpose the discovered schema is displayed in the terminal.

Afterwards, a new .xes log file will be generated next to the input log file.

The `artifacts` folder contains an example redo log, as well as a more verbose one, 
and an overview of the state the example database was in after process execution.

## Requirements

In order for the tool to run, a working installation of `Java 11` is required,
as well as `SBT 1.3.8` (or later) and `Scala 2.12.11`. SBT should be able to pick up the required
Scala version automatically.

For development, we recommend using `IntelliJ IDEA 2020.2` or newer.

## Data Generation

The data for evaluating this implementation was generated via 
[this](https://github.com/tom-lichtenstein/process-simulator) tool. It is designed to work with
a running Oracle DB 19c EE, which can be dowloaded [here](https://www.oracle.com/database/technologies/oracle-database-software-downloads.html#19c).

[This article](https://docs.oracle.com/en/database/oracle/oracle-database/18/sutil/oracle-logminer-utility.html#GUID-3417B738-374C-4EE3-B15C-3A66E01AE2B5)
provides information on how to extract the REDO log file. The database should be configured to archive the redo log prior to the process simulation. The output should be stored as a plain text file next to the cli tool.

To export the redo log in the correct format using sqlplus, the following formatting options must be set first:

```sql
column operation format a30
column username format a30
column sql_redo format a50
column timestamp format a25
column os_username format a20
set linesize 200
```

Additionally, the time format must be adapted:

```sql
ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-YYYY HH24:MI:SS';
```

Also the running headers must be removed:

```sql
SET HEADING OFF
```

For the redo log the information from the columns SQL_REDO, ROW_ID and TIMESTAMP is needed.
To generate the log directly from sqlplus, spool can be used. Spool stores the output of a query into a file.
Spool can be used as follows:

```sql
spool on;
spool ###target path###;
SELECT SQL_REDO, ROW_ID, TIMESTAMP FROM v$logmnr_contents WHERE TABLE_NAME='###table name###';
spool off;
```

Due to the formatting of sqlplus, line breaks may still have to be removed in the result.

## Packaging

A .jar file can be built by calling

```bash
$ sbt package
```

The binary will reside inside the `target` folder, and can be used instead of explicitly using SBT.
For this, only `Java 11` is required.
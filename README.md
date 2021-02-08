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

Strong primary key detection and events that include the values of the update statements can be achieved with

```bash
$ sbt run <path to log> --strict --includeUpdateValues
```

The redo log file should be a plain text file. The required format is described in the accompanying paper of this tool,
which is available upon request.

The tool will ask for a root class, which can be entered using the terminal. The root class must be one of the
discovered tables. For this purpose the discovered schema is displayed in the terminal.

Afterwards, a new .xes log file will be generated next to the input log file.

The `artifacts` folder contains an example redo log, as well as a more verbose one, and an overview of the state the
example database was in after process execution.

## Strong Primary Key Detection

This tool supports some heuristics for determining a good primary key candidate. Stricter checking, with use of these
heuristics, can be enabled by passing a `--strict`
flag during execution. Then, it is assured that

- the values of a PK column only ever increase over time
- the column name indicates a PK column

in addition to the existing check which ensures that

- the column values are unique over time

This can increase the accuracy of the primary key detection.

## Inclusion of Update Values in Event Names

Sometimes, especially when many values in updates are not unique but rather represent a finite amount of states, it can
be of interest to see the actual state represented in the event. For this, a `--includeUpdateValues` flag can be passed,
which enables the generation of more detailed event names from update statements.

## Optional Date Format Parameter

When parsing a Redo log, it can be cumbersome to manually change all timestamps to the expected format beforehand. The
default timestamp format is this: `"dd-MMM-yyyy HH:mm:ss"`. For changing the expected timestamp format,
a flag can be passed to the tool like so:

```bash
$ sbt run <path to log> --timestampPattern <timestamp pattern>
```

For extracting events out of the provided `redo_log.txt`, the default timestamp suffices. For the
provided `evaluation_log.txt`, use `"yyyy-MM-dd HH:mm:ss"`

## Requirements

In order for the tool to run, a working installation of `Java 11` is required, as well as `SBT 1.3.8` (or later)
and `Scala 2.12.11`. SBT should be able to pick up the required Scala version automatically.

For development, we recommend using `IntelliJ IDEA 2020.2` or newer.

## Data Generation

The data for evaluating this implementation was generated via
[this](https://github.com/tom-lichtenstein/process-simulator) tool. It is designed to work with a running Oracle DB 19c
EE, which can be
downloaded [here](https://www.oracle.com/database/technologies/oracle-database-software-downloads.html#19c).

[This article](https://docs.oracle.com/en/database/oracle/oracle-database/18/sutil/oracle-logminer-utility.html#GUID-3417B738-374C-4EE3-B15C-3A66E01AE2B5)
provides information on how to extract the REDO log file. The database should be configured to archive the redo log
prior to the process simulation. The output should be stored as a plain text file next to the cli tool.

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
$ sbt assembly
```

The binary will reside inside the `target` folder, and can be used instead of explicitly using SBT. For this,
only `Java 11` is required. If one were to use `sbt package`, scala dependencies would be missing and scala would be
required instead of java only. Then, the binary can be called with

```bash
$ java -jar redo-log-parser.jar <path to log> --<flags>
```
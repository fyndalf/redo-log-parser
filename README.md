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

Afterwards, a new .xes log file will be generated next to the input log file.

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

TODO: Tom - how to extract redo log from oracle in our format?

## Packaging

A .jar file can be built by calling

```bash
$ sbt package
```

The binary will reside inside the `target` folder, and can be used instead of explicitly using SBT.
For this, only `Java 11` is required.
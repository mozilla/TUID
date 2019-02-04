# Productionize the TUID service (GSOC Project 2019)

## Background

The TUID service is a Python/Flask/Sqlite webservice. It is responsible for mapping unique lines of source code to "Temporally Unique IDentifiers" which allows us to compare and merge coverage from separate revisions.  [Read more about the TUID project](CodeCoverage%20TUID.md)

The ActiveData-ETL project is responsible for converting raw coverage files coming out of Mozilla's CI, and converting them into JSON documents for insert in an Elasticsearch cluster. The project has hundreds of machines, and they request TUIDs for about 50 million source files daily.

## Problem

The TUID service can not handle the volume of requests coming from the ETL machines. The communication between the ETL machines and the TUID service a highly fault tolerant; even though the TUID service fails, the failure can be mitigated until we find proper solutions. 

## Solution

Make the service faster and more stable.

## Project Objectives 

The TUID service is a single-process Flask application that uses a Sqlite database. All solutions to making this faster are difficult:

* Use [gunicorn](https://gunicorn.org/) - this will allow multiple processes, but it will require some refactoring because there can only be one `[clogger](https://github.com/mozilla/TUID/blob/dev/tuid/clogger.py)`.
* Use Elasticsearch - We are not certain a Sqlite database file can handle multiple processes. If it can't, then we must put the TUID mappings into Elasticsearch. This will require changes to the data format and and changes to the storage methods.
* There are a [number of other issues](https://github.com/mozilla/TUID/issues) that must be resolved. 
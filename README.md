# Experimental TUID Project

TUID is an acronym for "temporally unique identifiers". These are numbers that effectively track "blame" throughout the source code.

|Branch      |Status   |
|------------|---------|
|master      | [![Build Status](https://travis-ci.org/mozilla/TUID.svg?branch=master)](https://travis-ci.org/mozilla/TUID) |
|dev         | [![Build Status](https://travis-ci.org/mozilla/TUID.svg?branch=dev)](https://travis-ci.org/mozilla/TUID)    |


## Overview

This is an attempt to provide a high speed cache for TUIDs. It is intended for use by CodeCoverage; mapping codecoverage by `tuid` rather than `(revsion, file, line)` triples.

More details can be gleaned from the [motivational document](https://github.com/mozilla/TUID/blob/dev/docs/CodeCoverage%20TUID.md).


## Running tests

After cloning the repo into `~/TUID`:

**Linux**

    cd ~/TUID
    pip install -r ./tests/requirements.txt
    export PYTHONPATH=.:vendor
    python -m pytest ./tests

**Windows**

    cd %userprofile%\TUID
    pip install -r .\tests\requirements.txt
    set PYTHONPATH=.;vendor
    python -m pytest .\tests

**Just one test**

Some tests take long, and you want to run just one of them. Here is an example:

    python -m pytest tests\test_basic.py::test_one_http_call_required


## Running the web service

You can run the web service locally with 

    cd ~/TUID
    export PYTHONPATH=.:vendor
    python tuid\app.py

The [`config.json`](./config.json) file has a `flask` property which is sent 
to the Flask service constructor. Notice the service is set to listen on 
port 5000. 

    "flask": {
        "host": "0.0.0.0",
        "port": 5000,
        "debug": false,
        "threaded": true,
        "processes": 1,
    }

The web service was designed to be part of a larger service. You can assign a 
route that points to the `tuid_endpoint()` method, and avoid the Flask
server construction.

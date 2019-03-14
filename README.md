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

Running any tests requires access to an Elastic Search cluster for `mo_hg` on localhost:9201.

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

**For Linux**

    python -m pytest tests/test_basic.py::test_one_http_call_required
    
**For windows**

    python -m pytest tests\test_basic.py::test_one_http_call_required

## Running the web application for development

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

## Deploying the web service

First, the server needs to be setup, which can be done by running
the server setup script `resources/scripts/setup_server.sh`, and then the
app can be setup using `resources/scripts/prod_app.sh`. If an error is
encountered when running `sudo supervisorctl`, try restarting it by
running the few commands in the server setup script.

## Using the web service

The `app.py` sets up a Flask application with an endpoint at `/tuid`. This 
endpoint models a database: It has one table called `files` and it can 
accept queries on that table. The number of queries supported is extremely 
limited:

    {
        "from":"files"
        "where": {"and": [
            {"eq": {"revision": "<REVISION>"}},
            {"in": {"path": ["<PATH1>", "<PATH2>", "...", "<PATHN>"]}}
        ]}
    }

Here is an example curl:

    curl -XGET http://localhost:5000/tuid -d "{\"from\":\"files\", \"where\":{\"and\":[{\"eq\":{\"revision\":\"9cb650de48f9\"}}, {\"eq\":{\"path\":\"modules/libpref/init/all.js\"}}]}}"

After some time (70sec as of March 23, 2018) we get a response (formatted 
and clipped for clarity):

    {
        "format":"table",
        "header":["path","tuids"],
        "data":[[
            "modules/libpref/init/all.js",
            [
                242488,
                245829,
                ...<snip>...
                243144
            ]
        ]]
    }

## Using the client

This repo includes a client (in `~/TUID/tuid/client.py`) that will send the 
necessary query to the service and cache the results in a local Sqlite 
database. This `TuidClient` was made for the ActiveData-ETL pipeline, so it 
has methods specifically suited for that project; but one method, called 
`get_tuid()`, you may find useful.





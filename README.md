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

Running any tests requires access to an Elastic Search cluster for `mo_hg` on localhost:9201. This requires [Elastic Search version 6.2.4](https://www.elastic.co/downloads/past-releases/elasticsearch-6-2-4). To look at the Elastic Search cluster, you can use Elasticsearch-head, [found here](https://github.com/mobz/elasticsearch-head).

After cloning the repo into `~/TUID`:

**Linux**

    cd ~/TUID
    pip install -r ./tests/requirements.txt
    pre-commit install
    export PYTHONPATH=.:vendor
    python -m pytest -m first_run --capture=no ./tests
    python -m pytest -m 'not first_run' --capture=no ./tests

**Windows**

    cd %userprofile%\TUID
    pip install -r .\tests\requirements.txt
    pre-commit install
    set PYTHONPATH=.;vendor
    python -m pytest -m first_run --capture=no tests
    python -m pytest -m 'not first_run' --capture=no tests

**Just one test**

Some tests take long, and you want to run just one of them. Here is an example:

**For Linux**

    python -m pytest tests/test_basic.py::test_one_http_call_required
    
**For windows**

    python -m pytest tests\test_basic.py::test_one_http_call_required

If there are issues that arise concerning a `private.json` file, you may be required to set the following environment variable: `TUID_CONFIG=tests/travis/config.json`

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
            {"eq": {"branch": "<BRANCH>"}},
            {"eq": {"revision": "<REVISION>"}},
            {"in": {"path": ["<PATH1>", "<PATH2>", "...", "<PATHN>"]}}
        ]}
    }

Here is an example curl:

    curl -XGET http://localhost:5000/tuid -d "{\"from\":\"files\", \"where\":{\"and\":[{\"eq\":{\"branch\":\"mozilla-central\"}}, {\"eq\":{\"revision\":\"9cb650de48f9\"}}, {\"eq\":{\"path\":\"modules/libpref/init/all.js\"}}]}}"

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

## Examples using this service

1. [Web-extension for Phabricator](https://github.com/gmierz/web-extensions/tree/master/tuid_annotate). See the README in that repo for installation instructions.


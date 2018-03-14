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

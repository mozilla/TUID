# CodeCoverage TUID Annotation Project

## Problems

* **We do not run coverage on every revision** - Collecting coverage on every revision would require about 100x more processing and storage.  
* **Per-test coverage is enormous** - Knowing the coverage of individual tests can inform us what tests should be run when code is changed.  Unfortunately collecting and storing at this resolution is 10000x more than what we do now. We can collect coverage for some subset of tests at a particular revision, but that has limited value given our constantly changing code.   
* **Coverage is variable** - Tests run at Firefox-scale have coverage variability because of environmental variability; which can include time of day, operating system latency, ordering of network responses, an many more. Running a test will not get you the full coverage for the test, rather some subset.
* **Coverage data is redundant** - There is an inevitable redundancy in the coverage data, as the same lines are hit, by the same suites, without fail, for months at time. With the right encoding, we can reduce our storage costs by removing redundant data; by recording only the changes in coverage.

## Proposal

Instead of recording coverage by (revision, file, line) triple we record coverage by TUID (temporally unique id) that replaces it, with the additional requirement that the *TUID is invariant when lines are moved*: 

* If a new line is added, a new TUID is assigned. 
* If a line is removed, the TUID is retired.
* If a line is changed, the TUID is retired and a new one assigned.  
* **If a line moves, because of changes above it, the TUID does not change.** 

More details can be found in [the repo used to demonstrate TUIDs](https://github.com/klahnakoski/diff-algebra#a-better-solution). Also, a [proof-of-concept TUID mapper](https://github.com/brockajones/TID) was built by a UCOSP student.

## Solutions

At a high level, storing coverage by TUID allows us to map coverage collected at one revision and map it to any other revision. 

* **We do not run coverage on every revision** - We need not collect coverage on every revision because it can be mapped from others we already collected on.
* **Per-test coverage is enormous** - TUIDS help enormously for per-test coverage which is a few orders of magnitude larger than what we collect now: A single Try run with per-test coverage enabled can be used for ?weeks? despite the ever-changing codebase.   
* **Coverage is variable** - If we can map coverage from one revision to another, then we can union coverage from multiple revisions. This aggregated coverage will have less variability and provide a more stable and accurate picture of our coverage.
* **Coverage data is redundant** - If we can map coverage from one revision to another, then we can also take their difference. Storing coverage differences will take less space.   
 
## Caveats

TUIDs can not replace actual coverage. TUID mappings do not even provide a best-guess of coverage given the available information: For example, code can change so that a block is never run, if that (uncovered) block calls another source file, the TUID mapping will wrongly consider that source file covered.

We do not believe this type of coverage anomalies will be a large problem. We already mitigate this problem by running coverage multiple time per day, so when this problem occurs it will not persist for long.

## Action

Integrate the [proof-of-concept TUID mapper](https://github.com/brockajones/TID). into the [ActiveData-ETL pipeline](https://github.com/klahnakoski/ActiveData-ETL) so that every coverage line is also marked with a TUID. The majority of the work will be dealing with the scale of our coverage; ensuring the mapping is fast enough that coverage data arrives in a timely manner. 

## Measurable Result

Like any refactoring, TUID annotation enables solutions rather than providing solutions directly. To prove that the TUID are in the database, and useful. We will build a prototype UI that will report "aggregate coverage": Coverage aggregated from the past N coverage runs. The coverage differences between aggregate coverage runs will be smaller than the coverage differences we see between individual coverage runs. This will be proof that we solved the **Coverage is variable** problem.

## Excluded

This project involves adding TUID annotations to code coverage records in ActiveData; all existing columns remain unchanged. As a consequence, nothing using the current coverage will break. Aside from the prototype, no front-end code is planned for this project.



# TUID project postmortem

*September 2018*

This document is meant to cover the challenges of this project. The project is not finished, so we reserve the right to add to this list, but it is well enough along to have something useful to say.

## Project Planning

[klahnakoski](https://github.com/klahnakoski) (me) had thought that a good API specification provided sufficient guidance to build the application; I was wrong. A detailed architecture meeting at the beginning would help with the multiple problems encountered during development. An architecture would clarify the internal workings of the program early.

1. It would help with reviews, which I did not consider important at first: Knowing the internal workings would make reviewing easier, instead of figuring out how the code is solving the problems I had not considered.
2. Reviews could be done sooner, and in smaller chunks, because the application would be more modular
3. A detailed architecture would improve the effort estimation of the whole project
4. Reviewing the implementation details early will expose bottlenecks (like getting annotations from hg.mo).

## Complications and Complexity

The TUID project development time single machine, millions of requests, using Python. In general, the bulk of the effort was going from a naive algorithm to one that scaled.

* Multiple request threads work together to update the data - Use database to provide transaction support; minimize multithreaded data access, and minimize data corruption
* Sqlite is not multithreaded - transactions had to be coordinated before they were sent to Sqlite
* hg.mo could not provide annotations as fast as TUID needed them - Clogger (Changeset Logger) built to apply changesets to TUID arrays rather than use annotations. Look-ahead to anticipate the revisions that will be requested. 
* Memory leak in one of [klahnakoski](https://github.com/klahnakoski)'s libraries.


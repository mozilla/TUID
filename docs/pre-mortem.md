# TUID project pre-mortem

This document is meant to cover the challenges of this project. The project is not finished, so we reserve the right to add to this list, but it is well enough along to have something useful to say.

## Project Planning

[klahnakoski](https://github.com/klahnakoski) (me) had thought that a good api specification provided sufficient guidance to build the application; that was wrong. A detailed architecture meeting at the beginning would help with the multiple problems encountered during development. An architecture would clarify the internal workings of the program early.

1. It would help with reviews, which I did not consider important at first: Knowing the internal workings would make reviewing easier, instead of figuring out how the code is solving problems I had not considered.
2. Reviews could be done sooner, and in smaller chunks, becasue the application would be more modular
3. A detailed architecture would improve the effort estimation of the whole project
4. Reviewing the implementation details early will expose bottlenecks (like getting annotations from hg.mo).

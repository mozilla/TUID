
# TUID Service Improvements

## Introduction
The TUID service is responsible for making Temporally Unique IDentifiers to identify unique lines of source code. TUIDs can be used for mapping code coverage between various revisions. The service is a single-process Flask application which was using SQLite database and was unable to handle a large volume of incoming requests from the ETL machines.

The aim of the proposal was to make the service more stable and faster. The proposal comprised of  two major changes:
1. Porting of application to use Elasticsearch instead of SQLite to enable parallelism. 
2. Make use of multiple processes to run this service in order to handle a large volume of requests simultaneously.

Elasticsearch porting was completed, but due to the nature of the project, the plan to incorporate multiprocessing was dropped. Instead, caching of the TUIDs was introduced such that TUIDs will be computed and stored in the ES when a new tip revision is made, which reduces the response time.


## Work
Porting the project to use Elasticsearch instead of SQLite was the starting point. Here, the difference between ES and SQLite was considered- the lack of transaction in ES, the way ES writes, the way ES deletes, etc. The recomputation of revnums was removed to allow negative revnums which eliminated unnecessary processing. We changed the structure of the annotations table to save TUID as an ordered list instead of a pair (line:tuid).

The original idea was to incorporate multi-processing with two Flask servers. Due to synchronization issues, the focus shifted to have multiple processes in the service using DB.  One process acts as a tuid generator and others communicate to the generator process via a table to get TUIDs. Work on this was in progress when the synchronization issue again was a blocker. The synchronization issue referred here is that coordination of TUID creation across all the processes, to ensure TUIDs are unique would remove all the speed that was gained with a multi-process service. Also for each file, ordering of application of the changesets had to be ensured (so no parallelism possible). Each changeset may have multiple files, so if the update was done on all the files in a changeset, it had to be ensured that no other process was doing the same. One solution was to assign each file to one process. The issue was co-ordination among processes while the distribution of the same. An important issue that needs to be addressed is what should be the status of the process that was assigned a particular file gets terminated or blocked. The mechanism through which other processes get notified needs to be defined. It should be clear what amount of time other processes should wait for a blocked process. If the original process is resurrected or unblocked, we should define how it behaves with the original file to which it was associated. The problem was not fully understood. There could be other hidden issues which were not obvious.

A decision was made not to have multiple processes. Instead, caching of TUIDs was started before the actual request comes in. A daemon was made to cache whenever ETL requests are not present in the system. To make the service even faster, work was done on the elimination of line origins logic from the service.

The code has not reached production yet and hence the ETL machines do not use the new changes for now.

Link to the PRs: https://github.com/mozilla/TUID/pulls?q=is%3Apr+author%3Aajupazhamayil


## Challenges
There were various challenges throughout the summer. These included Elasticsearch specialities, test errors, vendor library errors etc. Kyle and Greg were very helpful in pointing these out and mentoring to fix them. At some point, a conclusion was made that the problem with multi-processing was not going well, even with consistent efforts, due to synchronization issues and other problems mentioned above. But the performance aspect was achieved by having caching before the request arrives.

## The Experience
To get an opportunity like this was daunting but at the same time exhilarating. The experience while coding was joyous and so was the relationship with the mentors. The discussions about the problems were remarkable. It gave me a deep understanding of how to face and solve real-world problems.

Apart from coding, it was a wonderful learning experience. Taking this summer project helped to connect to a great community where people work in serene yet supportive morale and are always ready to help each other.

## Acknowledgement
First and foremost I would like to thank my mentors Kyle and Greg. Despite the significant time zone difference, they were always there to answer my questions and provide suggestions. And I can't overstate how wonderful it is to work with the Mozillian-community. Finally, I'd like to thank the GSoC program, which provided me with a platform where I could undertake this project and have a memorable experience in doing so.

## Future Work
* To move the code to production and make it work there.
* Timeout the requests if they take too long to respond.
* Optimize the existing code with the help of better libraries or refactoring.

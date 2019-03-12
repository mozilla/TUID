
# Using TUIDs for Code Coverage

TUID is an acronym for Temporally Unique Identifiers. 


## Background


### Blame Lines

Starting with blame

![](Tutorial%20Annotate.png)

https://hg.mozilla.org/mozilla-central/annotate/5bbe54174894/devtools/client/inspector/fonts/fonts.js


A look at the JSON output of the same reveals more about the magic used to make the lines

https://hg.mozilla.org/mozilla-central/json-annotate/5bbe54174894/devtools/client/inspector/fonts/fonts.js


The output is JSON, but I formatted as a table and removed most properties to make it easier to read
 

|  node | targetline | lineno |
|-------|------------|--------|
| 69d61 |      1     |    1   |
| 2d784 |      2     |    2   |
| 2d784 |      3     |    3   |
| 2d784 |      4     |    4   |
| 2d784 |      5     |    5   |
| 2d784 |      6     |    6   |
| 2d784 |      7     |    7   |
| 2d784 |      8     |    8   |
| e60ad |      9     |    9   |
| 2e915 |      9     |   10   |
| 010ba |     11     |   11   |
| 010ba |     13     |   12   |
| 010ba |     14     |   13   |
| 07bfb |     14     |   14   |
| 5bc43 |     16     |   15   |
| 010ba |     15     |   16   |
| 020ee |     16     |   17   |
| 2d784 |     11     |   18   |
| 010ba |     20     |   19   |
| 010ba |     21     |   20   |
| 010ba |     22     |   21   |
| 010ba |     23     |   22   |

The `targetline` is the line at the time of the writing. The `targetline` is not unique, but the `(node, targetline)` pair is unique. Note that the contents of the lines do not matter: We are tracking the life of a line through the source code;  we do not care if it a coverable line 

### TUID

The `(node, targetline)` pair is a logical way to track the life cycle of a line; the pair clearly refers to the creation point of the line, and helps readers track 
but it has a few inefficiencies in practice

 * The `node`, or revision number, is quite long
 * Being a pair of values adds a little overhead to storage and communication 

Instead of a pair, a unique identifier is used, and we name it a "temporally unique identifier" to reference the identifier does not change over the revisions of the source code.

|  TUID | lineno |
|-------|--------|
|   A   |    1   |
|   B   |    2   |
|   C   |    3   |
|   D   |    4   |
|   E   |    5   |
|   F   |    6   |
|   G   |    7   |
|   H   |    8   |
|   I   |    9   |
|   J   |   10   |
|   K   |   11   |
|   L   |   12   |
|   M   |   13   |
|   N   |   14   |
|   O   |   15   |
|   P   |   16   |
|   Q   |   17   |
|   R   |   18   |
|   S   |   19   |
|   T   |   20   |
|   U   |   21   |
|   V   |   22   |

TUIDs are more compact, but we loose the context of where the line comes from, and where it was originally written. This is a good thing:  It turns out the Firefox source is large, and its history is extensive, and we do not need to store all that history to assign TUIDs

If we have not seen a file before, we only need the number of lines in that file, to assign TUIDs. This means we can start in the middle of the repository graph and quickly assign tuids

## Using TUIDs to show coverage

### Coverage on a File

The common use case for code coverage is to show which lines of a file are covered and which are not. TUIDs can not do this in their own; this information has been abstracted away. To get back to real lines in real files, we need a tool to map TUIDs back to the `(revision, filename, line)` they represent.


This has been implemented as a public queryable endpoint that accepts `revision` and `filename`, and returns the ordered list of tuids 

<DESCRIBE TUID ENDPOINT>
<ADD EXAMPLE QUERY FOR OUR CANONICAL FILE>

### Coverage on another revision

Pulling the original coverage is more complex than using real line numbers, but TUIDs allow us to show coverage on revisions that were never run:

<ASK FOR COVERAGE FROM PREVIOUS REVISION OF CANONICAL FILE>

Notice that there are TUIDs we do not have coverage information on; these lines were removed, and they were never run, so no coverage could be collected.  As a human you may be able to deduce if those missing lines would have been covered if a coverage run was done on that past revision. 

## Nomenclature

I like to use the analogy of a light source placed at a particular revision on the source code timeline. This light will show all coverable lines for that revision; it will also reveal coverable lines on adjacent (past and future) revisions, but there may be some shadows that conceal coverage; as we get further from the lit revision, the cast shadows get larger.    

 * **Lit Revision** - A revision that coverage was collected on
 * **Shadow TUIDs** - TUIDs found on not-lit revisions, but not in the lit revision



## Coverage counting

### Coverage percent by file

Firefox coverage is collected by running hundreds of individual jobs: Each responsible for running some tests, but all capable of covering the same source files. This means, for "one coverage run" we will have hundreds of coverage records for a single source file. Each coverage record may cover different lines depending on the tests run, or the environment, or time of day, or state of the network, or decisions made on random variables. We would like to aggregate this coverage to a single percentage

We can union all the covered, and uncovered TUIDs:

    {
        "select": [
            {"value":"source.file.tuid_covered", "aggregate":"union"},
            {"value":"source.file.tuid_uncovered", "aggregate":"union"}
        ],
        "from":"coverage",
        "where": {"and":[
            {"eq":{"source.file.name":"some file"}},
            {"eq":{"repo.changeset.id12":"revision"}}
        ]}
    }

This is not the complete answer. There are some TUIDs in both lists; some jobs covered those lines, and some did not. Luckily we do not need to know what is uncovered, only the total coverable lines `tuid_covered | tuid_uncovered`, then calculate the percentage,

    percent_covered = len(tuid_covered) / len(tuid_covered | tuid_uncovered)  

We really do not need to know the TUIDs, we only need to count the number of unique ones; use the `cardinality` aggregate to count distinct TUIDs.

    {
        "select": [
            {
                "name": "total_covered",
                "value":"source.file.tuid_covered", 
                "aggregate":"cardinality"
            },
            {
                "name": "total_coverable",
                "value":{"union":["source.file.tuid_covered", "source.file.tuid_uncovered"],
                "aggregate":"cardinality"
            }
        ],
        "from":"coverage",
        "where": {"and":[
            {"eq":{"source.file.name":"some file"}},
            {"eq":{"repo.changeset.id12":"revision"}}
        ]}
    }

Then we can calculate 

    percent_covered = total_covered / total_coverable 


### Coverage percent for a collection of files

We may be interested in coverage for a collection of files. I this case, we select all files in a particular directory:

    {
        "select": [
            {
                "name": "total_covered",
                "value":"source.file.tuid_covered", 
                "aggregate":"cardinality"
            },
            {
                "name": "total_coverable",
                "value":{"union":["source.file.tuid_covered", "source.file.tuid_uncovered"],
                "aggregate":"cardinality"
            }
        ],
        "groupby": "source.file.name",
        "from":"coverage",
        "where": {"and":[
            {"prefix":{"source.file.name":"some prefix"}},
            {"eq":{"repo.changeset.id12":"revision"}}
        ]},
        "limit": 100
    }

If we are not interested in every file, only the overall coverage, then can remove the `groupby` to get a single number. We added `number of files` to count the number of unique filenames in our data. 

    {
        "select": [
            {
                "name": "total_covered",
                "value":"source.file.tuid_covered", 
                "aggregate":"cardinality"
            },
            {
                "name": "total_coverable",
                "value":{"union":["source.file.tuid_covered", "source.file.tuid_uncovered"],
                "aggregate":"cardinality"
            },
			{
                "name":"number of files",
                "source.file.name",
                "aggregate":"cardinality"
            }
        ],
        "from":"coverage",
        "where": {"and":[
            {"prefix":{"source.file.name":"some prefix"}},
            {"eq":{"repo.changeset.id12":"revision"}}
        ]}
    }

The important point here is the TUIDs are unique across files, where line numbers are not. If we see a TUID in one file, we know it will not be found in another file of the same revision; this allows us to aggregate the unique ones to effectively count all coverable lines over an arbitrary number of files.

### Total coverage 

Calculating the total coverage over a lit revision is a bit simpler; if only because we need not filter on the what files we are specifically interested in.  

    {
        "select": [
            {
                "name": "total_covered",
                "value":"source.file.tuid_covered", 
                "aggregate":"cardinality"
            },
            {
                "name": "total_coverable",
                "value":{"union":["source.file.tuid_covered", "source.file.tuid_uncovered"],
                "aggregate":"cardinality"
            },
			{
                "name":"number of files",
                "source.file.name",
                "aggregate":"cardinality"
            }
        ],
        "from":"coverage",
        "where": {"and":[
            {"eq":{"repo.changeset.id12":"revision"}}
        ]}
    }



## Coverage Aggregation

### The problem 
The lines reported covered by a coverage run is often different from one run to the next; even if it is the same revision. We can run coverage multiple times and union the covered lines for each file; the more often we run, the higher the chance we hit lines we previously missed. At Firefox scale, we never reach all possible paths, some situations are too rare; there is a law of diminishing returns, and the more we run the closer we get to an asymptotic limit.  The 

<insert chart of asymptotic coverage>

Not knowing the total coverage is a problem. This is made worse by the fact we do not have the computing resources to run coverage multiple times on the same revision.  Actually, we can not even afford to run coverage on every revision, we can barely afford to run coverage once every 100 <SOME ORDER OF MAGNITUDE> revisions. We must be satisfied with running coverage four times a day.   

### Solution

Our analogy of light sources continues to work when we consider multiple lit revisions; multiple light sources can remove many of the shadows and gives us better visibility into aggregate coverage. 

### Aggregate coverage on single file

We need not limit ourselves to coverage on a single revision. TUIDs are stable across revisions, so we can look at all the covered TUIDs over multiple lit revisions. If any TUIDs are covered, then they will show as covered in the aggregate. In this example, we aggregate coverage on all coverage runs in the past week:

    {
        "select": [
            {"value":"source.file.tuid_covered", "aggregate":"union"},
            {"value":"source.file.tuid_uncovered", "aggregate":"union"}
        ],
        "from":"coverage",
        "where": {"and":[
            {"eq":{"source.file.name":"some file"}},
            {"gte":{"repo.push.date":{"date":{"today-week"}}}}
        ]}
    }

### Aggregate coverage

We can aggregate coverage over time, in much the same way we did for a single file. This time we aggregate coverage over all files for the past week. 

    {
        "select": [
            {
                "name": "total_covered",
                "value":"source.file.tuid_covered", 
                "aggregate":"cardinality"
            },
            {
                "name": "total_coverable",
                "value":{"union":["source.file.tuid_covered", "source.file.tuid_uncovered"],
                "aggregate":"cardinality"
            },
			{
                "name":"number of files",
                "source.file.name",
                "aggregate":"cardinality"
            }
        ],
        "from":"coverage",
        "where": {"and":[
            {"gte":{"repo.push.date":{"date":"today-week"}}}
        ]}
    }


## 

### Coverage, past and future
### Same file over time coverage

## Coverage Difference

### A little trick (aka math)

ActiveData, a data warehouse, can not cross reference data. This limitation allows ActiveData to scale to multiple billions of records and remain fast, but it is a limitation. 

Logically, we would like to subtract the TUIDs covered by one revision and subtract the TUIDs covered by another, which will give us what the former covered that the latter missed. Subtracting coverage is a an example of cross referencing, and can not be done directly with a query.  

Fortunately, we can union sets; which gives us an indirect way to calculate differences. You may notice that all our examples used only two aggregates: The covered TUIDs and the coverable TUIDs; we did not mention the uncoverable TUIDs because they could not be calculated directly. We can calculate the uncoverable lines and the uncovered percent with local math:

    uncovered_tuid = coverable_tuid - covered_tuid  # this is set subtraction
    total_uncoverable = total_coverable - total_covered     


 




Show a file moved

Coverage differences


### Coverage on un-lit revisions

We defined "lit revisions" as revisions we have coverage information on. We can also calculate coverage on un-lit revisions. 

<NOTICE THE SHADOW TUIDS ON UN-LIT REVISIONS IMPACT %>


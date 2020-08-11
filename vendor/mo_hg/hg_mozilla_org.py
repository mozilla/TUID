# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import, division, unicode_literals

import re
from copy import copy

import mo_math
import mo_threads
from jx_elasticsearch import elasticsearch
from mo_dots import (
    Data,
    Null,
    coalesce,
    is_data,
    is_sequence,
    listwrap,
    set_default,
    unwraplist,
    wrap,
)
from mo_dots.lists import last
from mo_files import URL
from mo_future import binary_type, is_text, text, first
from mo_hg.parse import diff_to_json, diff_to_moves
from mo_hg.repos.changesets import Changeset
from mo_hg.repos.pushs import Push
from mo_hg.repos.revisions import Revision, revision_schema
from mo_http import http
from mo_kwargs import override
from mo_logs import Log, machine_metadata, strings
from mo_logs.exceptions import (
    Except,
    Explanation,
    suppress_exception,
)
from mo_math.randoms import Random
from mo_threads import Lock, Queue, THREAD_STOP, Thread, Till
from mo_times import Timer
from mo_times.dates import Date
from mo_times.durations import DAY, Duration, HOUR, MINUTE, SECOND
from pyLibrary.meta import cache

_hg_branches = None


def _count(values):
    return len(list(values))


def _late_imports():
    global _hg_branches

    from mo_hg import hg_branches as _hg_branches

    _ = _hg_branches


DEFAULT_LOCALE = "en-US"
DEBUG = False
DAEMON_DEBUG = False
DAEMON_HG_INTERVAL = 30  # HOW LONG TO WAIT BETWEEN HG REQUESTS (MAX)
DAEMON_WAIT_AFTER_TIMEOUT = 10 * 60  # IF WE SEE A TIMEOUT, THEN WAIT
WAIT_AFTER_NODE_FAILURE = (
    10 * 60
)  # IF WE SEE A NODE FAILURE OR CLUSTER FAILURE, THEN WAIT
WAIT_AFTER_CACHE_MISS = 30  # HOW LONG TO WAIT BETWEEN CACHE MISSES
DAEMON_DO_NO_SCAN = ["try"]  # SOME BRANCHES ARE NOT WORTH SCANNING
DAEMON_QUEUE_SIZE = 2 ** 15
DAEMON_RECENT_HG_PULL = 2  # DETERMINE IF WE GOT DATA FROM HG (RECENT), OR ES (OLDER)
MAX_TODO_AGE = DAY  # THE DAEMON WILL NEVER STOP SCANNING; DO NOT ADD OLD REVISIONS TO THE todo QUEUE
MIN_ETL_AGE = Date("03may2018").unix  # ARTIFACTS OLDER THAN THIS IN ES ARE REPLACED
UNKNOWN_PUSH = "Unknown push {{revision}}"
IGNORE_MERGE_DIFFS = True

MAX_DIFF_SIZE = 1000

last_called_url = {}


class HgMozillaOrg(object):
    """
    USE hg.mozilla.org FOR REPO INFORMATION
    USE ES AS A FASTER CACHE FOR THE SAME
    """

    @override
    def __init__(
        self,
        hg=None,  # hg CONNECTION INFO
        repo=None,  # CONNECTION INFO FOR ES CACHE
        use_cache=False,  # True IF WE WILL USE THE ES FOR DOWNLOADING BRANCHES
        kwargs=None,
    ):
        if not _hg_branches:
            _late_imports()

        if not is_text(repo.index):
            Log.error("Expecting 'index' parameter")
        self.repo_locker = Lock()
        self.moves_locker = Lock()
        self.todo = mo_threads.Queue("todo for hg daemon", max=DAEMON_QUEUE_SIZE)
        self.settings = kwargs
        self.hg = Data(
            url=hg.url,
            timeout=Duration(coalesce(hg.timeout, "30second")).seconds,
            retry={"times": 3, "sleep": DAEMON_HG_INTERVAL},
        )
        self.last_cache_miss = Date.now()

        # VERIFY CONNECTIVITY
        with Explanation("Test connect with hg"):
            http.head(self.settings.hg.url)

        set_default(repo, {"type": "revision", "schema": revision_schema,})
        kwargs.branches = set_default(
            {"index": repo.index + "-branches", "type": "branch"}, repo,
        )
        moves = set_default({"index": repo.index + "-moves"}, repo,)

        self.branches = _hg_branches.get_branches(kwargs=kwargs)
        cluster = elasticsearch.Cluster(kwargs=repo)
        self.repo = cluster.get_or_create_index(kwargs=repo)
        self.moves = cluster.get_or_create_index(kwargs=moves)

        def setup_es(please_stop):
            with suppress_exception:
                self.repo.add_alias()
            with suppress_exception:
                self.moves.add_alias()

            with suppress_exception:
                self.repo.set_refresh_interval(seconds=1)
            with suppress_exception:
                self.moves.set_refresh_interval(seconds=1)

        Thread.run("setup_es", setup_es)
        Thread.run("hg daemon", self._daemon)

    def _daemon(self, please_stop):
        while not please_stop:
            with Explanation("looking for work"):
                try:
                    branch, revisions, after = self.todo.pop(till=please_stop)
                except Exception as e:
                    if please_stop:
                        break
                    else:
                        raise e
                if branch.name in DAEMON_DO_NO_SCAN:
                    continue
                revisions = set(revisions)

                # FIND THE REVSIONS ON THIS BRANCH
                for r in list(revisions):
                    try:
                        rev = self.get_revision(
                            Revision(branch=branch, changeset={"id": r}),
                            None,  # local
                            False,  # get_diff
                            True,  # get_moves
                        )
                        if after and after > rev.etl.timestamp:
                            rev = self._get_from_hg(revision=rev)

                        if DAEMON_DEBUG:
                            Log.note(
                                "found revision with push date {{date|datetime}}",
                                date=rev.push.date,
                            )
                        revisions.discard(r)

                        if rev.etl.timestamp > Date.now() - (
                            DAEMON_RECENT_HG_PULL * SECOND
                        ):
                            # SOME PUSHES ARE BIG, RUNNING THE RISK OTHER MACHINES ARE
                            # ALSO INTERESTED AND PERFORMING THE SAME SCAN. THIS DELAY
                            # WILL HAVE SMALL EFFECT ON THE MAJORITY OF SMALL PUSHES
                            # https://bugzilla.mozilla.org/show_bug.cgi?id=1417720
                            Till(seconds=Random.float(DAEMON_HG_INTERVAL * 2)).wait()

                    except Exception as e:
                        Log.warning(
                            "Scanning {{branch}} {{revision|left(12)}}",
                            branch=branch.name,
                            revision=r,
                            cause=e,
                        )
                        if "Read timed out" in e:
                            Till(seconds=DAEMON_WAIT_AFTER_TIMEOUT).wait()

                # FIND ANY BRANCH THAT MAY HAVE THIS REVISION
                for r in list(revisions):
                    self._find_revision(r)

    @cache(duration=HOUR, lock=True)
    def get_revision(self, revision, locale=None, get_diff=False, get_moves=True):
        """
        EXPECTING INCOMPLETE revision OBJECT
        RETURNS revision
        """
        rev = revision.changeset.id
        if not rev:
            return Null
        elif rev == "None":
            return Null
        elif revision.branch.name == None:
            return Null
        locale = coalesce(locale, revision.branch.locale, DEFAULT_LOCALE)
        output = self._get_from_elasticsearch(
            revision, locale=locale, get_diff=get_diff, get_moves=get_moves
        )
        if output:
            if not get_diff:  # DIFF IS BIG, DO NOT KEEP IT IF NOT NEEDED
                output.changeset.diff = None
            if not get_moves:
                output.changeset.moves = None
            DEBUG and Log.note(
                "Got hg ({{branch}}, {{locale}}, {{revision}}) from ES",
                branch=output.branch.name,
                locale=locale,
                revision=output.changeset.id,
            )
            if output.push.date:
                return output

        return self._get_from_hg(revision, locale, get_diff, get_moves)

    def _get_from_hg(self, revision, locale=None, get_diff=False, get_moves=True):
        # RATE LIMIT CALLS TO HG (CACHE MISSES)
        next_cache_miss = self.last_cache_miss + (
            Random.float(WAIT_AFTER_CACHE_MISS * 2) * SECOND
        )
        self.last_cache_miss = Date.now()
        if next_cache_miss > self.last_cache_miss:
            Log.note(
                "delaying next hg call for {{seconds|round(decimal=1)}} seconds",
                seconds=next_cache_miss - self.last_cache_miss,
            )
            Till(till=next_cache_miss.unix).wait()

        # CLEAN UP BRANCH NAME
        found_revision = copy(revision)
        if isinstance(found_revision.branch, (text, binary_type)):
            lower_name = found_revision.branch.lower()
        else:
            lower_name = found_revision.branch.name.lower()

        if not lower_name:
            Log.error("Defective revision? {{rev|json}}", rev=found_revision.branch)

        b = found_revision.branch = self.branches[(lower_name, locale)]
        if not b:
            b = found_revision.branch = self.branches[(lower_name, DEFAULT_LOCALE)]
            if not b:
                Log.warning(
                    "can not find branch ({{branch}}, {{locale}})",
                    branch=lower_name,
                    locale=locale,
                )
                return Null

        # REFRESH BRANCHES, IF TOO OLD
        if Date.now() - Date(b.etl.timestamp) > _hg_branches.OLD_BRANCH:
            self.branches = _hg_branches.get_branches(kwargs=self.settings)

        # FIND THE PUSH
        push = self._get_push(found_revision.branch, found_revision.changeset.id)
        id12 = found_revision.changeset.id[0:12]
        base_url = URL(found_revision.branch.url)

        with Explanation("get revision from {{url}}", url=base_url, debug=DEBUG):
            raw_rev2 = Null
            automation_details = Null
            try:
                raw_rev1 = self._get_raw_json_info((base_url / "json-info") + {"node": id12})
                raw_rev2 = self._get_raw_json_rev(base_url / "json-rev" / id12)
                automation_details = self._get_raw_json_rev(base_url / "json-automationrelevance" / id12)
            except Exception as e:
                if "Hg denies it exists" in e:
                    raw_rev1 = Data(node=revision.changeset.id)
                else:
                    raise e

            raw_rev3_changeset = first(
                r for r in automation_details.changesets if r.node[:12] == id12
            )
            if last(automation_details.changesets) != raw_rev3_changeset:
                Log.note("interesting")

            output = self._normalize_revision(
                set_default(raw_rev1, raw_rev2, raw_rev3_changeset),
                found_revision,
                push,
                get_diff,
                get_moves,
            )
            if output.push.date >= Date.now() - MAX_TODO_AGE:
                self.todo.extend(
                    [
                        (output.branch, listwrap(output.parents), None),
                        (output.branch, listwrap(output.children), None),
                        (
                            output.branch,
                            listwrap(output.backsoutnodes),
                            output.push.date,
                        ),
                    ]
                )

            if not get_diff:  # DIFF IS BIG, DO NOT KEEP IT IF NOT NEEDED
                output.changeset.diff = None
            if not get_moves:
                output.changeset.moves = None

        return output

    def _get_from_elasticsearch(
        self, revision, locale=None, get_diff=False, get_moves=True
    ):
        """
        MAKE CALL TO ES
        """
        rev = revision.changeset.id
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"changeset.id12": rev[0:12]}},
                        {"term": {"branch.name": revision.branch.name}},
                        {
                            "term": {
                                "branch.locale": coalesce(
                                    locale, revision.branch.locale, DEFAULT_LOCALE
                                )
                            }
                        },
                        {"range": {"etl.timestamp": {"gt": MIN_ETL_AGE}}},
                    ]
                }
            },
            "size": 20,
        }

        for attempt in range(3):
            try:
                with Timer("get from elasticsearch", too_long=2 * SECOND):
                    if get_moves:
                        with self.moves_locker:
                            docs = self.moves.search(query).hits.hits
                    else:
                        with self.repo_locker:
                            docs = self.repo.search(query).hits.hits
                if len(docs) == 0:
                    return None
                best = docs[0]._source
                if len(docs) > 1:
                    for d in docs:
                        if d._id.endswith(d._source.branch.locale):
                            best = d._source
                    Log.warning("expecting no more than one document")
                return best
            except Exception as e:
                e = Except.wrap(e)
                if (
                    "EsRejectedExecutionException[rejected execution (queue capacity"
                    in e
                ):
                    (Till(seconds=Random.int(30))).wait()
                    continue
                else:
                    Log.warning(
                        "Bad ES call, waiting for {{num}} seconds",
                        num=WAIT_AFTER_NODE_FAILURE,
                        cause=e,
                    )
                    Till(seconds=WAIT_AFTER_NODE_FAILURE).wait()
                    continue

        Log.warning("ES did not deliver, fall back to HG")
        return None

    @cache(duration=HOUR, lock=True)
    def _get_raw_json_info(self, url):
        raw_revs = self._get_and_retry(url)
        if "(not in 'served' subset)" in raw_revs:
            Log.error("Tried {{url}}. Hg denies it exists.", url=url)
        if is_text(raw_revs) and raw_revs.startswith("unknown revision '"):
            Log.error("Tried {{url}}. Hg denies it exists.", url=url)
        if len(raw_revs) != 1:
            Log.error("do not know what to do")
        return raw_revs.values()[0]

    @cache(duration=HOUR, lock=True)
    def _get_raw_json_rev(self, url):
        raw_rev = self._get_and_retry(url)
        return raw_rev

    @cache(duration=HOUR, lock=True)
    def _get_push(self, branch, changeset_id):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"branch.name": branch.name}},
                        {"prefix": {"changeset.id": changeset_id[0:12]}},
                    ]
                }
            },
            "size": 1,
        }

        try:
            # ALWAYS TRY ES FIRST
            with self.repo_locker:
                response = self.repo.search(query)
                json_push = response.hits.hits[0]._source.push
            if json_push:
                return json_push
        except Exception:
            pass

        url = branch.url.rstrip("/") + "/json-pushes?full=1&changeset=" + changeset_id
        with Explanation("Pulling pushlog from {{url}}", url=url, debug=DEBUG):
            data = self._get_and_retry(url)
            # QUEUE UP THE OTHER CHANGESETS IN THE PUSH
            self.todo.add(
                (branch, [c.node for cs in data.values().changesets for c in cs], None)
            )
            pushes = [
                Push(id=int(index), date=_push.date, user=_push.user)
                for index, _push in data.items()
            ]

        if len(pushes) == 0:
            return Null
        elif len(pushes) == 1:
            return pushes[0]
        else:
            Log.error("do not know what to do")

    def _normalize_revision(self, r, found_revision, push, get_diff, get_moves):
        new_names = set(r.keys()) - KNOWN_TAGS
        if new_names and not r.tags:
            Log.warning(
                "hg is returning new property names {{names|quote}} for {{changeset}} from {{url}}",
                names=new_names,
                changeset=r.node,
                url=found_revision.branch.url,
            )

        changeset = Changeset(
            id=r.node,
            id12=r.node[0:12],
            author=coalesce(r.author, r.user),
            description=strings.limit(coalesce(r.description, r.desc), 2000),
            date=parse_hg_date(r.date),
            files=r.files,
            backedoutby=r.backedoutby,
            backsoutnodes=r.backsoutnodes,
            bug=mo_math.UNION(
                ([int(b) for b in r.bugs.no], self._extract_bug_id(r.description))
            ),
        )
        rev = Revision(
            branch=found_revision.branch,
            index=r.rev,
            changeset=changeset,
            parents=set(r.parents),
            children=set(r.children),
            push=push,
            phase=r.phase,
            bookmarks=unwraplist(r.bookmarks),
            landingsystem=r.landingsystem,
            etl={"timestamp": Date.now().unix, "machine": machine_metadata},
        )
        rev = elasticsearch.scrub(rev)

        r.pushuser = None
        r.pushdate = None
        r.pushid = None
        r.node = None
        r.user = None
        r.desc = None
        r.description = None
        r.date = None
        r.files = None
        r.backedoutby = None
        r.parents = None
        r.children = None
        r.bookmarks = None
        r.landingsystem = None
        r.extra = None
        r.author = None
        r.pushhead = None
        r.reviewers = None
        r.bugs = None
        r.treeherderrepourl = None
        r.backsoutnodes = None
        r.treeherderrepo = None
        r.perfherderurl = None
        r.branch = None
        r.phase = None
        r.rev = None
        r.tags = None

        set_default(rev, r)

        # ADD THE DIFF
        if get_diff:
            rev.changeset.diff = self._get_json_diff_from_hg(rev)

        try:
            _id = (
                coalesce(rev.changeset.id12, "")
                + "-"
                + rev.branch.name
                + "-"
                + coalesce(rev.branch.locale, DEFAULT_LOCALE)
            )
            with self.repo_locker:
                self.repo.add({"id": _id, "value": rev})
            if get_moves:
                rev.changeset.moves = self._get_moves_from_hg(rev)
                with self.moves_locker:
                    self.moves.add({"id": _id, "value": rev})
        except Exception as e:
            e = Except.wrap(e)
            Log.warning(
                "Did not save to ES, waiting {{duration}} seconds",
                duration=WAIT_AFTER_NODE_FAILURE,
                cause=e,
            )
            Till(seconds=WAIT_AFTER_NODE_FAILURE).wait()
            if "FORBIDDEN/12/index read-only" in e:
                pass  # KNOWN FAILURE MODE

        return rev

    def _get_and_retry(self, url):
        try:
            data = http.get_json(**set_default({"url": url}, self.hg))
            if data.error.startswith("unknown revision"):
                Log.error(UNKNOWN_PUSH, revision=strings.between(data.error, "'", "'"))
            if is_text(data) and data.startswith("unknown revision"):
                Log.error(UNKNOWN_PUSH, revision=strings.between(data, "'", "'"))
            # branch.url = _trim(url)  # RECORD THIS SUCCESS IN THE BRANCH
            return data
        except Exception as e:
            path = url.split("/")
            if path[3] == "l10n-central":
                # FROM https://hg.mozilla.org/l10n-central/tr/json-pushes?full=1&changeset=a6eeb28458fd
                # TO   https://hg.mozilla.org/mozilla-central/json-pushes?full=1&changeset=a6eeb28458fd
                path = path[0:3] + ["mozilla-central"] + path[5:]
                return self._get_and_retry("/".join(path))
            elif len(path) > 5 and path[5] == "mozilla-aurora":
                # FROM https://hg.mozilla.org/releases/l10n/mozilla-aurora/pt-PT/json-pushes?full=1&changeset=b44a8c68fc60
                # TO   https://hg.mozilla.org/releases/mozilla-aurora/json-pushes?full=1&changeset=b44a8c68fc60
                path = path[0:4] + ["mozilla-aurora"] + path[7:]
                return self._get_and_retry("/".join(path))
            elif len(path) > 5 and path[5] == "mozilla-beta":
                # FROM https://hg.mozilla.org/releases/l10n/mozilla-beta/lt/json-pushes?full=1&changeset=03fbf7556c94
                # TO   https://hg.mozilla.org/releases/mozilla-beta/json-pushes?full=1&changeset=b44a8c68fc60
                path = path[0:4] + ["mozilla-beta"] + path[7:]
                return self._get_and_retry("/".join(path))
            elif len(path) > 7 and path[5] == "mozilla-release":
                # FROM https://hg.mozilla.org/releases/l10n/mozilla-release/en-GB/json-pushes?full=1&changeset=57f513ab03308adc7aa02cc2ea8d73fe56ae644b
                # TO   https://hg.mozilla.org/releases/mozilla-release/json-pushes?full=1&changeset=57f513ab03308adc7aa02cc2ea8d73fe56ae644b
                path = path[0:4] + ["mozilla-release"] + path[7:]
                return self._get_and_retry("/".join(path))
            elif len(path) > 5 and path[4] == "autoland":
                # FROM https://hg.mozilla.org/build/autoland/json-pushes?full=1&changeset=3ccccf8e5036179a3178437cabc154b5e04b333d
                # TO  https://hg.mozilla.org/integration/autoland/json-pushes?full=1&changeset=3ccccf8e5036179a3178437cabc154b5e04b333d
                path = path[0:3] + ["try"] + path[5:]
                return self._get_and_retry("/".join(path))

            raise e

    @cache(duration=HOUR, lock=True)
    def _find_revision(self, revision):
        please_stop = False
        locker = Lock()
        output = []
        queue = Queue("repo branches", max=2000)
        queue.extend(
            b
            for b in self.branches
            if b.locale == DEFAULT_LOCALE
            and b.name in ["try", "mozilla-inbound", "autoland"]
        )
        queue.add(THREAD_STOP)

        def _find(please_stop):
            for b in queue:
                if please_stop:
                    return
                try:
                    url = b.url.rstrip("/") + "/json-info?node=" + revision
                    rev = self.get_revision(
                        Revision(branch=b, changeset={"id": revision})
                    )
                    with locker:
                        output.append(rev)
                    Log.note("Revision found at {{url}}", url=url)
                except Exception:
                    pass

        threads = [
            Thread.run("find changeset " + text(i), _find, please_stop=please_stop)
            for i in range(3)
        ]

        for t in threads:
            t.join()

        return output

    def _extract_bug_id(self, description):
        """
        LOOK INTO description to FIND bug_id
        """
        if description == None:
            return None
        match = re.findall(r"[Bb](?:ug)?\s*([0-9]{5,7})", description)
        if match:
            return int(match[0])
        return None

    def _get_json_diff_from_hg(self, revision):
        """
        :param revision: INCOMPLETE REVISION OBJECT
        :return:
        """

        @cache(duration=MINUTE, lock=True)
        def inner(changeset_id):
            # ALWAYS TRY ES FIRST
            json_diff = _get_changeset_from_es(self.repo, changeset_id).changeset.diff
            if json_diff:
                return json_diff
            url = URL(revision.branch.url) / "raw-rev" / changeset_id
            DEBUG and Log.note("get unified diff from {{url}}", url=url)
            try:
                response = http.get(url)
                try:
                    diff = response.content.decode("utf8")
                except Exception as e:
                    diff = response.content.decode("latin1")

                # File("tests/resources/big.patch").write_bytes(response.content)
                json_diff = diff_to_json(diff)
                num_changes = _count(c for f in json_diff for c in f.changes)
                if json_diff:
                    if (
                        IGNORE_MERGE_DIFFS
                        and revision.changeset.description.startswith("merge ")
                    ):
                        return None  # IGNORE THE MERGE CHANGESETS
                    elif num_changes < MAX_DIFF_SIZE:
                        return json_diff
                    else:
                        Log.warning(
                            "Revision at {{url}} has a diff with {{num}} changes, ignored",
                            url=url,
                            num=num_changes,
                        )
                        for file in json_diff:
                            file.changes = None
                        return json_diff
            except Exception as e:
                Log.warning("could not get unified diff from {{url}}", url=url, cause=e)

        return inner(revision.changeset.id)

    def _get_moves_from_hg(self, revision):
        """
        :param revision: INCOMPLETE REVISION OBJECT
        :return:
        """

        @cache(duration=MINUTE, lock=True)
        def inner(changeset_id):
            # ALWAYS TRY ES FIRST
            moves = _get_changeset_from_es(self.moves, changeset_id).changeset.moves
            if moves:
                return moves

            url = URL(revision.branch.url) / "raw-rev" / changeset_id
            DEBUG and Log.note("get unified diff from {{url}}", url=url)
            try:
                # THE ENCODING DOES NOT MATTER BECAUSE WE ONLY USE THE '+', '-' PREFIXES IN THE DIFF
                moves = http.get(url).content.decode("latin1")
                return diff_to_moves(text(moves))
            except Exception as e:
                Log.warning("could not get unified diff from {{url}}", url=url, cause=e)

        return inner(revision.changeset.id)

    def _get_source_code_from_hg(self, revision, file_path):
        response = http.get(
            URL(revision.branch.url) / "raw-file" / revision.changeset.id / file_path
        )
        return response.content.decode("utf8", "replace")


def _trim(url):
    return url.split("/json-pushes?")[0].split("/json-info?")[0].split("/json-rev/")[0]


def _get_changeset_from_es(es, changeset_id):
    try:
        response = es.search(
            {
                "query": {
                    "bool": {
                        "must": [
                            {"prefix": {"changeset.id": changeset_id}},
                            {"range": {"etl.timestamp": {"gt": MIN_ETL_AGE}}},
                        ]
                    }
                },
                "size": 1,
            }
        )
        return response.hits.hits[0]._source
    except Exception:
        return Null


def parse_hg_date(date):
    if is_text(date):
        return Date(date)
    elif is_sequence(date):
        # FIRST IN TUPLE (timestamp, time_zone) TUPLE, WHERE timestamp IS GMT
        return Date(date[0])
    else:
        Log.error("Can not deal with date like {{date|json}}", date=date)


def minimize_repo(repo):
    """
    RETURN A MINIMAL VERSION OF THIS CHANGESET
    """
    if repo == None:
        return Null
    output = wrap(_copy_but(repo, _exclude_from_repo))
    output.changeset.description = strings.limit(output.changeset.description, 1000)
    return output


_exclude_from_repo = Data()
for k in [
    "changeset.files",
    "changeset.diff",
    "changeset.moves",
    "etl",
    "branch.last_used",
    "branch.description",
    "branch.etl",
    "branch.parent_name",
    "children",
    "parents",
    "phase",
    "bookmarks",
    "tags",
]:
    _exclude_from_repo[k] = True
_exclude_from_repo = _exclude_from_repo


def _copy_but(value, exclude):
    output = {}
    for k, v in value.items():
        e = exclude.get(k, {})
        if e != True:
            if is_data(v):
                v2 = _copy_but(v, e)
                if v2 != None:
                    output[k] = v2
            elif v != None:
                output[k] = v
    return output if output else None


KNOWN_TAGS = {
    "rev",
    "node",
    "user",
    "description",
    "desc",
    "date",
    "files",
    "backedoutby",
    "parents",
    "children",
    "branch",
    "tags",
    "pushuser",
    "pushdate",
    "pushid",
    "phase",
    "bookmarks",
    "landingsystem",
    "extra",
    "author",
    "pushhead",
    "reviewers",
    "bugs",
    "treeherderrepourl",
    "backsoutnodes",
    "treeherderrepo",
    "perfherderurl",
}

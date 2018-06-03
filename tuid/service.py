# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import gc

from mo_dots import Null, coalesce, wrap
from mo_future import text_type
from mo_hg.hg_mozilla_org import HgMozillaOrg
from mo_kwargs import override
from mo_logs import Log
from mo_threads import Till, Thread
from mo_times.durations import SECOND
from pyLibrary.env import http
from pyLibrary.sql import sql_list, sql_iso
from pyLibrary.sql.sqlite import quote_value
from tuid import sql
from tuid.util import MISSING, TuidMap

DEBUG = False
RETRY = {"times": 3, "sleep": 5}
SQL_ANN_BATCH_SIZE = 5
SQL_BATCH_SIZE = 500
DAEMON_WAIT_AT_NEWEST = 30 * SECOND # Time to wait at the newest revision before polling again.

GET_TUID_QUERY = "SELECT tuid FROM temporal WHERE file=? and revision=? and line=?"

GET_ANNOTATION_QUERY = "SELECT annotation FROM annotations WHERE revision=? and file=?"

GET_LATEST_MODIFICATION = "SELECT revision FROM latestFileMod WHERE file=?"


class TUIDService:

    @override
    def __init__(self, database, hg, hg_cache=None, conn=None, kwargs=None):
        try:
            self.config = kwargs

            self.conn = conn if conn else sql.Sql(self.config.database.name)
            self.hg_cache = HgMozillaOrg(kwargs=self.config.hg_cache, use_cache=True) if self.config.hg_cache else Null

            if not self.conn.get_one("SELECT name FROM sqlite_master WHERE type='table';"):
                self.init_db()

            self.next_tuid = coalesce(self.conn.get_one("SELECT max(tuid)+1 FROM temporal")[0], 1)
        except Exception as e:
            Log.error("can not setup service", cause=e)


    def tuid(self):
        """
        :return: next tuid
        """
        try:
            return self.next_tuid
        finally:
            self.next_tuid += 1


    def init_db(self):
        '''
        Creates all the tables, and indexes needed for the service.

        :return: None
        '''
        with self.conn.transaction() as t:
            t.execute('''
            CREATE TABLE temporal (
                tuid     INTEGER,
                revision CHAR(12) NOT NULL,
                file     TEXT,
                line     INTEGER
            );''')

            t.execute('''
            CREATE TABLE annotations (
                revision       CHAR(12) NOT NULL,
                file           TEXT,
                annotation     TEXT,
                PRIMARY KEY(revision, file)
            );''')

            # Used in frontier updating
            t.execute('''
            CREATE TABLE latestFileMod (
                file           TEXT,
                revision       CHAR(12) NOT NULL,
                PRIMARY KEY(file)
            );''')

            t.execute("CREATE UNIQUE INDEX temporal_rev_file ON temporal(revision, file, line)")
        Log.note("Tables created successfully")


    def _dummy_tuid_exists(self, transaction, file_name, rev):
        # True if dummy, false if not.
        # None means there is no entry.
        return None != transaction.get_one("select 1 from temporal where file=? and revision=? and line=?",
                                         (file_name, rev, 0))


    def _dummy_annotate_exists(self, transaction, file_name, rev):
        # True if dummy, false if not.
        # None means there is no entry.
        return None != transaction.get_one("select 1 from annotations where file=? and revision=? and annotation=?",
                                         (file_name, rev, ''))


    def insert_tuid_dummy(self, transaction, rev, file_name, commit=True):
        # Inserts a dummy tuid: (-1,rev,file_name,0)
        if not self._dummy_tuid_exists(transaction, file_name, rev):
            transaction.execute(
                "INSERT INTO temporal (tuid, revision, file, line) VALUES (?, ?, ?, ?)",
                (-1, rev[:12], file_name, 0)
            )
            # if commit:
            #     self.conn.commit()
        return MISSING


    def insert_annotate_dummy(self, transaction, rev, file_name, commit=True):
        # Inserts annotation dummy: (rev, file, '')
        if not self._dummy_annotate_exists(transaction, file_name, rev):
            transaction.execute(
                "INSERT INTO annotations (revision, file, annotation) VALUES (?, ?, ?)",
                (rev[:12], file_name, ''))
            if commit:
                self.conn.commit()
        return [(rev[:12], file_name, '')]


    def _get_annotation(self, transaction, rev, file):
        # Returns an annotation if it exists
        return transaction.get_one(GET_ANNOTATION_QUERY, (rev, file))


    def _get_one_tuid(self, transaction, cset, path, line):
        # Returns a single TUID if it exists
        return transaction.get_one("select 1 from temporal where revision=? and file=? and line=?",
                                 (cset, path, int(line)))


    def _get_latest_revision(self, transaction, file):
        # Returns the latest revision that we
        # have information on the requested file.
        return transaction.get_one(GET_LATEST_MODIFICATION, (file,))


    def stringify_tuids(self, tuid_list):
        # Turns the TuidMap list to a string for storage in
        # the annotations table.
        return "\n".join([','.join([str(x.tuid), str(x.line)]) for x in tuid_list])


    def destringify_tuids(self, tuids_string):
        # Builds up TuidMap list from annotation cache entry.
        lines = str(tuids_string[0]).splitlines()
        line_origins = []
        for line in lines:
            entry = line.split(',')
            line_origins.append(TuidMap(int(entry[0].replace("'", "")), int(entry[1].replace("'", ""))))
        return line_origins


    # Gets a diff from a particular revision from https://hg.mozilla.org/
    def _get_hg_diff(self, cset, repo=None):
        if repo is None:
            repo = self.config.hg.branch
        tmp = self.hg_cache.get_revision(
            wrap({
                "changeset": {"id": cset},
                "branch": {"name": repo}
            }),
            None, False, True
        )
        output = tmp['changeset']['moves']
        return output


    # Gets an annotated file from a particular revision from https://hg.mozilla.org/
    def _get_hg_annotate(self, cset, file, annotated_files, thread_num, repo, please_stop=None):
        url = 'https://hg.mozilla.org/' + repo + '/json-annotate/' + cset + "/" + file
        if DEBUG:
            Log.note("HG: {{url}}", url=url)

        # Ensure we get the annotate before continuing
        try:
            annotated_files[thread_num] = http.get_json(url, retry=RETRY)
        except Exception as e:
            annotated_files[thread_num] = []
            Log.warning("Unexpected error while trying to get annotate for {{url}}", url=url, cause=e)
        return


    def get_diffs(self, csets, repo=None):
        # Get all the diffs
        if repo is None:
            repo = self.config.hg.branch

        list_diffs = []
        for cset in csets:
            list_diffs.append({'cset': cset, 'diff': self._get_hg_diff(cset,repo=repo)})
        return list_diffs


    def get_tuids_from_revision(self, revision):
        """
        Gets the TUIDs for the files modified by a revision.

        :param revision: revision to get files from
        :return: list of (file, list(tuids)) tuples
        """
        result = []
        URL_TO_FILES = 'https://hg.mozilla.org/' + self.config.hg.branch + '/json-info/' + revision
        try:
            mozobject = http.get_json(url=URL_TO_FILES, retry=RETRY)
        except Exception as e:
            Log.warning("Unexpected error trying to get file list for revision {{revision}}", cause=e)
            return None

        files = mozobject[revision]['files']

        results = self.get_tuids(files, revision)
        return results


    def get_tuids_from_files(self, files, revision, going_forward=False, repo=None):
        """
        Gets the TUIDs for a set of files, at a given revision.
        list(tuids) is an array of tuids, one tuid for each line, in order, and `null` if no tuid assigned

        Uses frontier updating to build and maintain the tuids for
        the given set of files. Use changelog to determine what revisions
        to process and get the files that need to be updated by looking
        at the diffs. If the latestFileMod table is empty, for any file,
        we perform an annotation-based update.

        This function assumes the newest file names are given, if they
        are not, then no TUIDs are returned for that file.

        :param files: list of files
        :param revision: revision to get files at
        :return: list of (file, list(tuids)) tuples
        """
        if repo is None:
            repo = self.config.hg.branch

        if repo in ('try',):
            # We don't need to keep latest file revisions
            # and other related things for this condition.
            with self.conn.transaction() as transaction:
                return self._get_tuids_from_files_try_branch(transaction, files, revision)
        elif repo not in ('mozilla-central',):
            Log.warning("The requested branch `{{repo}}` is unsupported", repo=repo)
            return [(file, []) for file in files]

        result = []
        revision = revision[:12]
        files = [file.lstrip('/') for file in files]
        frontier_update_list = []

        total = len(files)
        latestFileMod_inserts = {}
        new_files = []

        with self.conn.transaction() as transaction:
            log_existing_files = []
            for count, file in enumerate(files):
                # Go through all requested files and
                # either update their frontier or add
                # them to the DB through an initial annotation.

                if DEBUG:
                    Log.note(" {{percent|percent(decimal=0)}}|{{file}}", file=file, percent=count / total)

                latest_rev = self._get_latest_revision(transaction, file)

                # Check if the file has already been collected at
                # this revision and get the result if so
                already_ann = self._get_annotation(transaction, revision, file)
                if already_ann:
                    result.append((file,self.destringify_tuids(already_ann)))
                    if going_forward:
                        latestFileMod_inserts[file] = (file, revision)
                    log_existing_files.append('exists|' + file)
                    continue
                elif already_ann[0] == '':
                    result.append((file,[]))
                    if going_forward:
                        latestFileMod_inserts[file] = (file, revision)
                    log_existing_files.append('removed|' + file)
                    continue

                if (latest_rev and latest_rev[0] != revision):
                    # File has a frontier, let's update it
                    if DEBUG:
                        Log.note("Will update frontier for file {{file}}.", file=file)
                    frontier_update_list.append((file, latest_rev[0]))
                elif latest_rev == revision:
                    tmp_res = self.destringify_tuids(self._get_annotation(transaction, latest_rev[0], file))
                    result.append((file, tmp_res))
                    Log.note(
                        "Frontier update - already exists in DB with state `exists`: " +
                        "{{rev}}|{{file}} ",
                        file=file, rev=revision
                    )
                else:
                    Log.note(
                        "Frontier update - adding: " +
                        "{{rev}}|{{file}} ",
                        file=file, rev=revision
                    )
                    new_files.append(file)

            if DEBUG:
                Log.note(
                    "Frontier update - already exist in DB: " +
                    "{{rev}} || {{file_list}} ",
                    file_list=str(log_existing_files), rev=revision
                )
            else:
                Log.note(
                    "Frontier update - already exist in DB for {{rev}}: " +
                    "{{count}}/{{total}} | {{percent}}",
                    count=str(len(log_existing_files)), total=str(len(files)),
                    rev=revision, percent=str(len(log_existing_files)/len(files))
                )

            if len(new_files) > 0:
                # File has never been seen before, get it's initial
                # annotation to work from in the future.
                tmp_res = self.get_tuids(new_files, revision, commit=False)
                if tmp_res:
                    result.extend(tmp_res)
                else:
                    Log.note("Error occured for files " + str(new_files) + " in revision " + revision)

                # If this file has not been seen before,
                # add it to the latest modifications, else
                # it's already in there so update its past
                # revisions.
                for file in new_files:
                    latestFileMod_inserts[file] = (file, revision)

            # If we have files that need to have their frontier updated
            if len(frontier_update_list) > 0:
                tmp = self._update_file_frontiers(transaction, frontier_update_list, revision, going_forward=going_forward)
                result.extend(tmp)

            Log.note("Finished updating frontiers. Updating DB table `latestFileMod`...")
            if len(latestFileMod_inserts) > 0:
                count = 0
                listed_inserts = [latestFileMod_inserts[i] for i in latestFileMod_inserts]
                while count < len(listed_inserts):
                    inserts_list = listed_inserts[count:count + SQL_BATCH_SIZE]
                    count += SQL_BATCH_SIZE
                    #try:
                    transaction.execute(
                        "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
                        sql_list(
                            sql_iso(sql_list(map(quote_value, i)))
                            for i in inserts_list
                        )
                    )
                    #except Exception as e:
                    #    Log.warning("Error inserting into latestFileMods, {{error}}", error=e)

        return result


    def _apply_diff(self, transaction, annotation, diff, cset, file):
        '''
        Using an annotation ([(tuid,line)] - array
        of TuidMap objects), we change the line numbers to
        reflect a given diff and return them. diff must
        be a diff object returned from get_diff(cset, file).
        Only for going forward in time, not back.

        :param annotation: list of TuidMap objects
        :param diff: unified diff from get_diff
        :param cset: revision to apply diff at
        :param file: name of file diff is applied to
        :return:
        '''
        # Add all added lines into the DB.
        list_to_insert = []
        new_ann = [x for x in annotation]
        new_ann.sort(key=lambda x: x.line)

        def add_one(tl_tuple, lines):
            start = tl_tuple.line
            return lines[:start-1] + [tl_tuple] + [TuidMap(tmap.tuid, int(tmap.line) + 1) for tmap in lines[start-1:]]

        def remove_one(start, lines):
            return lines[:start-1] + [TuidMap(tmap.tuid, int(tmap.line) - 1) for tmap in lines[start:]]

        for f_proc in diff:
            if f_proc['new'].name.lstrip('/') != file:
                continue

            f_diff = f_proc['changes']
            for change in f_diff:
                if change.action == '+':
                    tuid_tmp = self._get_one_tuid(transaction, cset, file, change.line+1)
                    if not tuid_tmp:
                        new_tuid = self.tuid()
                        list_to_insert.append((new_tuid, cset, file, change.line+1))
                    else:
                        new_tuid = tuid_tmp[0]
                    new_ann = add_one(TuidMap(new_tuid, change.line+1), new_ann)
                elif change.action == '-':
                    new_ann = remove_one(change.line+1, new_ann)
            break # Found the file, exit searching

        if len(list_to_insert) > 0:
            count = 0
            while count < len(list_to_insert):
                inserts_list = list_to_insert[count:count + SQL_BATCH_SIZE]
                count += SQL_BATCH_SIZE
                transaction.execute(
                    "INSERT INTO temporal (tuid, revision, file, line)" +
                    " VALUES " +
                    sql_list(sql_iso(sql_list(map(quote_value, tp))) for tp in inserts_list)
                )

        return new_ann


    def _get_tuids_from_files_try_branch(self, transaction, files, revision):
        '''
        Gets files from a try revision. It abuses the idea that try pushes
        will come from various, but stable points (if people make many
        pushes on that revision). Furthermore, updates are generally done
        to a revision that should eventually have tuids already in the DB
        (i.e. overtime as people update to revisions that have a tuid annotation).

        :param files: Files to query.
        :param revision: Revision to get them at.
        :return: List of (file, tuids) tuples.
        '''

        # Check if the files were already annotated.
        repo = 'try'
        result = []
        log_existing_files = []
        files_to_update = []
        for file in files:
            already_ann = self._get_annotation(transaction, revision, file)
            if already_ann:
                result.append((file, self.destringify_tuids(already_ann)))
                log_existing_files.append('exists|' + file)
                continue
            elif already_ann[0] == '':
                result.append((file, []))
                log_existing_files.append('removed|' + file)
                continue
            else:
                files_to_update.append(file)

        if len(log_existing_files) > 0:
            Log.note("Try revision run - existing entries: {{count}}/{{total}} | {{percent}}",
                     count=str(len(log_existing_files)), total=str(len(files)),
                     percent=str(100*(len(log_existing_files)/len(files)))
            )

        if len(files_to_update) <= 0:
            Log.note("Found all files for try revision request: {{cset}}", cset=revision)
            return result

        # There are files to process, so let's find all the diffs.
        found_mc_patch = False
        diffs_to_get = [] # Will contain diffs in reverse order of application
        curr_rev = revision
        mc_revision = ''
        while not found_mc_patch:
            jsonrev_url = 'https://hg.mozilla.org/' + repo + '/json-rev/' + curr_rev
            try:
                Log.note("Searching through changelog {{url}}", url=jsonrev_url)
                clog_obj = http.get_json(jsonrev_url, retry=RETRY)
                if isinstance(clog_obj, (text_type, str)):
                    Log.error(
                        "Revision {{cset}} does not exist in the {{branch}} branch",
                        cset=curr_rev, branch=repo
                    )
                if 'phase' not in clog_obj:
                    Log.warning(
                        "Unexpected error getting changset-log for {{url}}: `phase` entry cannot be found.",
                        url=jsonrev_url
                    )
                    return [(file, []) for file in files]
            except Exception as e:
                Log.warning(
                    "Unexpected error getting changset-log for {{url}}: {{error}}",
                    url=jsonrev_url, error=e
                )
                return [(file, []) for file in files]

            # When `phase` is public, the patch is (assumed to be)
            # in any repo other than try.
            if clog_obj['phase'] == 'public':
                found_mc_patch = True
                mc_revision = curr_rev
                continue
            elif clog_obj['phase'] == 'draft':
                diffs_to_get.append(curr_rev)
            else:
                Log.warning(
                    "Unknown `phase` state `{{state}}` encountered at revision {{cset}}",
                    cset=curr_rev, state=clog_obj['phase']
                )
                return [(file, []) for file in files]
            curr_rev = clog_obj['parents'][0][:12]

        added_files = {}
        removed_files = {}
        files_to_process = {}

        Log.note("Gathering diffs for: {{csets}}", csets=str(diffs_to_get))
        all_diffs = self.get_diffs(diffs_to_get, repo=repo)
        # Build a dict for faster access to the diffs
        parsed_diffs = {entry['cset']: entry['diff'] for entry in all_diffs}
        for csets_diff in all_diffs:
            cset_len12 = csets_diff['cset']
            parsed_diff = csets_diff['diff']

            for f_added in parsed_diff:
                # Get new entries for removed files.
                new_name = f_added['new'].name.lstrip('/')
                old_name = f_added['old'].name.lstrip('/')

                # If we don't need this file, skip it
                if new_name not in files_to_update:
                    # If the file was removed, set a
                    # flag and return no tuids later.
                    if new_name == 'dev/null':
                        removed_files[old_name] = True
                    continue

                if old_name == 'dev/null':
                    added_files[new_name] = True
                    continue

                if new_name in files_to_process:
                    files_to_process[new_name].append(cset_len12)
                else:
                    files_to_process[new_name] = [cset_len12]

        # We've found a good patch (a public one), get it
        # for all files and apply the patch's onto it.
        curr_annotations = self.get_tuids(files, mc_revision, commit=False)
        curr_annots_dict = {el[0]: el[1] for el in curr_annotations}

        anns_to_get = []
        ann_inserts = []
        for file in files_to_update:
            if file in added_files:
                Log.note("Try revision run - added: {{file}}", file=file)
                anns_to_get.append(file)
            elif file in removed_files:
                Log.note("Try revision run - removed: {{file}}", file=file)
                ann_inserts.append((revision, file, ''))
                result.append((file, []))
            elif file in files_to_process:
                # Reverse the list, we always find the newest diff first
                Log.note("Try revision run - modified: {{file}}", file=file)
                csets_to_proc = files_to_process[file][::-1]
                old_ann = curr_annots_dict[file]

                # Apply all the diffs
                tmp_res = old_ann
                for i in csets_to_proc:
                    tmp_res = self._apply_diff(transaction, tmp_res, parsed_diffs[i], i, file)

                ann_inserts.append((revision, file, self.stringify_tuids(tmp_res)))
                result.append((file, tmp_res))
            else:
                # Nothing changed with the file, use it's current annotation
                Log.note("Try revision run - not modified: {{file}}", file=file)
                ann_inserts.append((revision, file, self.stringify_tuids(curr_annots_dict[file])))
                result.append((file, curr_annots_dict[file]))

        if len(anns_to_get) > 0:
            result.extend(self.get_tuids(anns_to_get, revision, repo=repo))

        if len(ann_inserts) > 0:
            count = 0
            ann_inserts = list(set(ann_inserts))
            while count < len(ann_inserts):
                tmp_inserts = ann_inserts[count:count + SQL_ANN_BATCH_SIZE]
                count += SQL_ANN_BATCH_SIZE
                try:
                    transaction.execute(
                        "INSERT INTO annotations (revision, file, annotation) VALUES " +
                        sql_list(sql_iso(sql_list(map(quote_value, i))) for i in tmp_inserts)
                    )
                except Exception as e:
                    Log.note("Error inserting into annotations table: {{inserting}}", inserting=tmp_inserts)
                    Log.error("Error found: {{cause}}", cause=e)

        return result


    def _update_file_frontiers(self, transaction, frontier_list, revision, max_csets_proc=30,
                               going_forward=False):
        '''
        Update the frontier for all given files, up to the given revision.

        Built for quick continuous _forward_ updating of large sets
        of files of TUIDs. Backward updating should be done through
        get_tuids(files, revision). If we cannot find a frontier, we will
        stop looking after max_csets_proc and update all files at the given
        revision.

        :param frontier_list: list of files to update
        :param revision: revision to update files to
        :param max_csets_proc: maximum number of changeset logs to look through
                               to find past frontiers.
        :param going_forward: If we know the requested revision is in front
                              of the latest revision use this flag. Used when
                              the frontier is too far away. If this is not set and
                              a frontier is too far, the latest revision will not
                              be updated.
        :return: list of (file, list(tuids)) tuples
        '''

        # Get the changelogs and revisions until we find the
        # last one we've seen, and get the modified files in
        # each one.

        # Holds the files modified up to the last frontiers.
        files_to_process = {}

        # Holds all known frontiers
        latest_csets = {cset: True for cset in list(set([rev for (file,rev) in frontier_list]))}
        file_to_frontier = {tp[0]: tp[1] for tp in frontier_list}
        found_last_frontier = False
        if len(latest_csets) <= 1 and frontier_list[0][1] == revision:
            # If the latest revision is the requested revision,
            # continue to the tuid querys.
            found_last_frontier = True

        final_rev = revision  # Revision we are searching from
        csets_proced = 0
        diffs_cache = []
        diffs_to_frontier = {cset: [] for cset in latest_csets}
        removed_files = {}
        if DEBUG:
            Log.note("Searching for the following frontiers: {{csets}}", csets=str([cset for cset in latest_csets]))

        tmp = [cset for cset in latest_csets]
        Log.note("Searching for frontier(s): {{frontier}} ", frontier=str(tmp))
        Log.note("HG URL: {{url}}", url='https://hg.mozilla.org/' + self.config.hg.branch + '/rev/' + tmp[0])
        while not found_last_frontier:
            # Get a changelog
            clog_url = 'https://hg.mozilla.org/' + self.config.hg.branch + '/json-log/' + final_rev
            try:
                Log.note("Searching through changelog {{url}}", url=clog_url)
                clog_obj = http.get_json(clog_url, retry=RETRY)
                if isinstance(clog_obj, (text_type, str)):
                    Log.error(
                        "Revision {{cset}} does not exist in the {{branch}} branch",
                        cset=final_rev, branch=self.config.hg.branch
                    )
            except Exception as e:
                Log.error("Unexpected error getting changset-log for {{url}}: {{error}}", url=clog_url, error=e)

            # For each changeset/node
            still_looking = True
            for count, clog_cset in enumerate(clog_obj['changesets']):
                if count >= len(clog_obj['changesets']) - 1:
                    break
                cset_len12 = clog_cset['node'][:12]

                if still_looking:
                    if cset_len12 in latest_csets:
                        # Found a frontier, remove it from search list.
                        latest_csets[cset_len12] = False
                        still_looking = any([latest_csets[cs] for cs in latest_csets])

                        if not still_looking:
                            break

                    # If there are still frontiers left to explore,
                    # add the files this node modifies to the processing list.
                    diffs_cache.append(cset_len12)

                    # Used to prevent gathering diffs we don't need.
                    for cset in diffs_to_frontier:
                        if latest_csets[cset]:
                            diffs_to_frontier[cset].append(cset_len12)

                if cset_len12 in latest_csets:
                    # Found a frontier, remove it from search list.
                    latest_csets[cset_len12] = False
                    still_looking = any([latest_csets[cs] for cs in latest_csets])

            csets_proced += 1
            if not still_looking:
                # End searching
                found_last_frontier = True
            elif csets_proced >= max_csets_proc:
                # In this case, all files need to be updated to this revision to ensure
                # line ordering consistency (between past, and future) when a revision
                # that is in the past is asked for.
                found_last_frontier = True

                files_to_process = {f: [revision] for (f,r) in frontier_list}

            if not found_last_frontier:
                # Go to the next log page
                final_rev = clog_obj['changesets'][len(clog_obj['changesets'])-1]['node'][:12]

        if not still_looking:
            Log.note("Found all frontiers: {{frontiers_list}}", frontiers_list=str([i for i in latest_csets]))
        else:
            Log.note("Found frontiers: {{found}}", found=str([i for i in latest_csets if not latest_csets[i]]))
            Log.note("Did not find frontiers: {{found}}", found=
                str([i for i in latest_csets if latest_csets[i]])
            )

        added_files = {}
        parsed_diffs = {}
        if not all([latest_csets[cs] for cs in latest_csets]): # If there is at least one frontier that was found

            # Only get diffs that are needed (if any frontiers were not found)
            diffs_cache = []
            for cset in diffs_to_frontier:
                if not latest_csets[cset]:
                    diffs_cache.extend(diffs_to_frontier[cset])

            Log.note("Gathering diffs for: {{csets}}", csets=str(diffs_cache))
            all_diffs = self.get_diffs(diffs_cache)

            # Build a dict for faster access to the diffs
            parsed_diffs = {entry['cset']: entry['diff'] for entry in all_diffs}

            for csets_diff in all_diffs:
                cset_len12 = csets_diff['cset']
                parsed_diff = csets_diff['diff']

                for f_added in parsed_diff:
                    # Get new entries for removed files.
                    new_name = f_added['new'].name.lstrip('/')
                    old_name = f_added['old'].name.lstrip('/')

                    # If we don't need this file, skip it
                    if new_name not in file_to_frontier:
                        # If the file was removed, set a
                        # flag and return no tuids later.
                        if new_name == 'dev/null':
                            removed_files[old_name] = True
                        continue

                    if old_name == 'dev/null':
                        added_files[new_name] = True

                    # At this point, file is in the database, and is
                    # asked to be processed, and we are still
                    # searching for the last frontier.

                    # If we are past the frontier for this file,
                    # or if we are at the frontier skip it.
                    if file_to_frontier[new_name] == '':
                        continue
                    if file_to_frontier[new_name] == cset_len12:
                        file_to_frontier[new_name] = ''
                        continue

                    # Skip diffs that change file names, this is the first
                    # annotate entry to the new file_name and it doesn't do
                    # anything to the old other than bring it to new.
                    # We should never make it to this point unless there was an error elsewhere
                    # because any frontier for the new_name file should be at this revision or
                    # further ahead - never earlier.
                    if old_name != new_name:
                        Log.warning("Should not have made it here, can't find a frontier for {{file}}", file=new_name)
                        continue

                    if new_name in files_to_process:
                        files_to_process[new_name].append(cset_len12)
                    else:
                        files_to_process[new_name] = [cset_len12]

        # Process each file that needs it based on the
        # files_to_process list.
        result = []
        ann_inserts = []
        latestFileMod_inserts = {}
        anns_to_get = []
        total = len(frontier_list)
        for count, file_n_rev in enumerate(frontier_list):
            file = file_n_rev[0]
            rev = file_n_rev[1]

            if latest_csets[rev]:
                # If we were still looking by the end, get a new
                # annotation for this file.
                anns_to_get.append(file)
                if going_forward:
                    latestFileMod_inserts[file] = (file, revision)
                Log.note("Frontier update - can't find frontier {lost_frontier}: " +
                         "{{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ",
                         count=count, total=total, file=file, rev=revision, percent=count / total,
                         lost_frontier=rev
                )
                continue

            # If the file was modified, get it's newest
            # annotation and update the file.
            proc_rev = rev
            proc = False
            if file in files_to_process:
                proc = True
                proc_rev = revision

            modified = True
            tmp_res = None
            if proc and file not in removed_files:
                # Process this file using the diffs found

                # Reverse the list, we always find the newest diff first
                csets_to_proc = files_to_process[file][::-1]
                old_ann = self.destringify_tuids(self._get_annotation(transaction, rev, file))

                # Apply all the diffs
                tmp_res = old_ann
                for i in csets_to_proc:
                    tmp_res = self._apply_diff(transaction, tmp_res, parsed_diffs[i], i, file)

                ann_inserts.append((revision, file, self.stringify_tuids(tmp_res)))
                Log.note("Frontier update - modified: {{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ", count=count,
                                                total=total, file=file, rev=proc_rev, percent=count / total)
            elif file not in removed_files:
                old_ann = self._get_annotation(transaction, rev, file)
                if old_ann is None or (old_ann == '' and file in added_files):
                    # File is new (likely from an error), or re-added - we need to create
                    # a new initial entry for this file.
                    tmp_res = self.get_tuids(file, proc_rev, commit=False)[0][1]
                    Log.note(
                        "Frontier update - readded: {{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ",
                        count=count, total=total, file=file,
                        rev=proc_rev, percent=count / total
                    )
                else:
                    # File was not modified since last
                    # known revision
                    tmp_res = self.destringify_tuids(old_ann) if old_ann != '' else []
                    ann_inserts.append((revision, file, old_ann[0]))
                    modified = False
                    Log.note(
                        "Frontier update - not modified: {{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ",
                        count=count, total=total, file=file,
                        rev=proc_rev, percent=count / total
                    )
            else:
                # File was removed
                ann_inserts.append((revision, file, ''))
                tmp_res = None
                Log.note(
                    "Frontier update - removed: {{count}}/{{total}} - {{percent|percent(decimal=0)}} | {{rev}}|{{file}} ",
                    count=count, total=total, file=file,
                    rev=proc_rev, percent=count / total
                )

            if tmp_res:
                result.append((file, tmp_res))
                if proc_rev != revision and not modified:
                    # If the file hasn't changed up to this revision,
                    # reinsert it with the same previous annotate.
                    if not self._get_annotation(transaction, revision, file):
                        annotate = self.destringify_tuids(self._get_annotation(transaction, rev, file))
                        ann_inserts.append((revision, file, self.stringify_tuids(annotate)))
            else:
                Log.note("Error occured for file {{file}} in revision {{revision}}", file=file, revision=proc_rev)
                result.append((file, []))

            # If we have found all frontiers, update to the
            # latest revision. Otherwise, the requested
            # revision is too far away (can't be sure
            # if it's past). Unless we are told that we are
            # going forward.
            latest_rev = revision
            latestFileMod_inserts[file] = (file, latest_rev)

        if len(anns_to_get) > 0:
            result.extend(self.get_tuids(anns_to_get, revision, commit=False))

        Log.note("Updating DB tables `latestFileMod` and `annotations`...")
        if len(latestFileMod_inserts) > 0:
            count = 0
            listed_inserts = [latestFileMod_inserts[i] for i in latestFileMod_inserts]
            while count < len(listed_inserts):
                tmp_inserts = listed_inserts[count:count + SQL_BATCH_SIZE]
                count += SQL_BATCH_SIZE
                transaction.execute(
                    "INSERT OR REPLACE INTO latestFileMod (file, revision) VALUES " +
                    sql_list(sql_iso(sql_list(map(quote_value, i))) for i in tmp_inserts)
                )

        if len(ann_inserts) > 0:
            count = 0
            ann_inserts = list(set(ann_inserts))
            while count < len(ann_inserts):
                tmp_inserts = ann_inserts[count:count + SQL_ANN_BATCH_SIZE]
                count += SQL_ANN_BATCH_SIZE
                try:
                    transaction.execute(
                        "INSERT INTO annotations (revision, file, annotation) VALUES " +
                        sql_list(sql_iso(sql_list(map(quote_value, i))) for i in tmp_inserts)
                    )
                except Exception as e:
                    Log.note("Error inserting into annotations table: {{inserting}}", inserting=tmp_inserts)
                    Log.error("Error found: {{cause}}", cause=e)

        return result


    def _update_file_changesets(self, transaction, annotated_lines):
        '''
        Inserts new lines from all changesets in the given annotation.

        :param annotated_lines: Response from annotation request from HGMO
        :return: None
        '''
        quickfill_list = []

        for anline in annotated_lines:
            cset = anline['node'][:12]
            if not self._get_one_tuid(transaction, cset, anline['abspath'], int(anline['targetline'])):
                quickfill_list.append((cset, anline['abspath'], int(anline['targetline'])))
        self._quick_update_file_changeset(transaction, list(set(quickfill_list)))


    def _quick_update_file_changeset(self, transaction, qf_list):
        '''
        Updates temporal table to include any new TUIDs.

        :param qf_list: List to insert
        :return: None
        '''
        count = 0
        while count < len(qf_list):
            tmp_qf_list = qf_list[count:count+SQL_BATCH_SIZE]
            count += SQL_BATCH_SIZE
            transaction.execute(
                "INSERT INTO temporal (tuid, revision, file, line)" +
                " VALUES " +
                sql_list(sql_iso(sql_list(map(quote_value, (self.tuid(), i[0], i[1], i[2])))) for i in tmp_qf_list)
            )


    def get_tuids(self, files, revision, commit=True, chunk=50, repo=None):
        if repo is None:
            repo = self.config.hg.branch

        count = 0
        results = []
        while count < len(files):
            with self.conn.transaction() as transaction:
                results.extend(self._get_tuids(transaction, files[count:count+chunk], revision, commit=commit, repo=repo))
                count += chunk
        return results

    def _get_tuids(self, transaction, files, revision, commit=True, repo=None):
        '''
        Returns (TUID, line) tuples for a given file at a given revision.

        Uses json-annotate to find all lines in this revision, then it updates
        the database with any missing revisions for the file changes listed
        in annotate. Then, we use the information from annotate coupled with the
        diff information that was inserted into the DB to return TUIDs. This way
        we don't have to deal with child, parents, dates, etc..

        :param files: list of files to get
        :param revision: revision at which to get the file
        :param commit: True to commit new TUIDs else False
        :return: List of TuidMap objects
        '''

        # For a single file, there is no need
        # to put it in an array when given.
        if not isinstance(files, list):
            files = [files]

        revision = revision[:12]
        for count, file in enumerate(files):
            files[count] = file.lstrip('/')

        results = []
        annotations_to_get = []
        for file in files:
            already_ann = self._get_annotation(transaction, revision, file)
            if already_ann:
                results.append((file, self.destringify_tuids(already_ann)))
            elif already_ann[0] == '':
                results.append((file, []))
            else:
                annotations_to_get.append(file)

        if not annotations_to_get:
            return results

        # Get all the annotations in parallel
        num_files = len(annotations_to_get)
        annotated_files = [None] * num_files
        threads = [None] * num_files

        # Get all the annotations in parallel
        annotated_files = [None] * len(annotations_to_get)
        threads = [
            Thread.run(str(i), self._get_hg_annotate, revision, annotations_to_get[i], annotated_files, i, repo)
            for i, a in enumerate(annotations_to_get)
        ]
        for t in threads:
            t.join()

        for fcount, annotated_object in enumerate(annotated_files):
            existing_tuids = {}
            tmp_tuids = []
            file = annotations_to_get[fcount]

            # If it's not defined at this revision, we need to add it in
            errored = False
            if isinstance(annotated_object, (text_type, str)):
                errored = True
                Log.warning("{{file}} does not exist in the revision {{cset}}", cset=revision, file=file)
            elif annotated_object is None:
                Log.warning(
                    "Unexpected error getting annotation for: {{file}} in the revision {{cset}}",
                    cset=revision, file=file
                )
                errored = True
            elif 'annotate' not in annotated_object:
                Log.warning(
                    "Missing annotate, type got: {{ann_type}}, expecting:dict returned when getting " +
                    "annotation for: {{file}} in the revision {{cset}}",
                    cset=revision, file=file, ann_type=type(annotated_object)
                )
                errored = True

            if errored:
                Log.note("Inserting dummy entry...")
                self.insert_tuid_dummy(transaction, revision, file, commit=commit)
                self.insert_annotate_dummy(transaction, revision, file, commit=commit)
                results.append((file, []))
                continue

            # Gather all missing csets and the
            # corresponding lines.
            annotated_lines = []
            line_origins = []
            existing_tuids = {}
            for node in annotated_object['annotate']:
                cset_len12 = node['node'][:12]

                # If the cset is not in the database, process it
                #
                # Use the 'abspath' field to determine the current filename in
                # case it has changed.
                tuid_tmp = transaction.get_one(GET_TUID_QUERY, (node['abspath'], cset_len12, int(node['targetline'])))
                if (not tuid_tmp):
                    annotated_lines.append(node)
                else:
                    existing_tuids[int(node['lineno'])] = tuid_tmp[0]
                # Used to gather TUIDs later
                line_origins.append((node['abspath'], cset_len12, int(node['targetline'])))

            # Update DB with any revisions found in annotated
            # object that are not in the DB.
            if len(annotated_lines) > 0:
                # If we are using get_tuids within another transaction
                self._update_file_changesets(transaction, annotated_lines)

            # Get the TUIDs for each line (can probably be optimized with a join)
            tuids = tmp_tuids
            for line_num in range(1, len(line_origins) + 1):
                if line_num in existing_tuids:
                    tuids.append(TuidMap(existing_tuids[line_num], line_num))
                    continue
                try:
                    tuid_tmp = transaction.get_one(GET_TUID_QUERY,
                                                 line_origins[line_num - 1])
                    # Return dummy line if we can't find the TUID for this entry
                    # (likely because of an error from insertion).
                    if tuid_tmp:
                        tuids.append(TuidMap(tuid_tmp[0], line_num))
                    else:
                        tuids.append(MISSING)
                except Exception as e:
                    Log.note("Unexpected error searching {{cause}}", cause=e)

            transaction.execute(
                "INSERT INTO annotations (revision, file, annotation) VALUES (?,?,?)",
                (
                    revision,
                    file,
                    self.stringify_tuids(tuids)
                )
            )

            # if commit:
            #     self.conn.commit()

            results.append((file, tuids))

        # Try to prevent some memory leaking
        # with these calls.
        del threads
        del annotated_files
        gc.collect()

        return results


    def _daemon(self, please_stop, only_coverage_revisions=False):
        '''
        Runs continuously to prefill the temporal and
        annotations table with the coverage revisions*.

        * A coverage revision is a revision which has had
        code coverage run on it.

        :param please_stop: Used to stop the daemon
        :return: None
        '''
        while not please_stop:
            # Get all known files and their latest revisions on the frontier
            files_n_revs = self.conn.get("SELECT file, revision FROM latestFileMod")

            # Split these files into groups of revisions to make it
            # easier to update them. If we group them together, we
            # may end up updating groups that are new back to older
            # revisions.
            revs = {rev: [] for rev in set([file_n_rev[1] for file_n_rev in files_n_revs])}
            for file_n_rev in files_n_revs:
                revs[file_n_rev[1]].append(file_n_rev[0])

            # Go through each frontier and update it
            ran_changesets = False
            coverage_revisions = None
            for frontier in revs:
                if please_stop:
                    return

                files = revs[frontier]

                # Go through changeset logs until we find the last
                # known frontier for this revision group.
                csets = []
                final_rev = ''
                found_last_frontier = False
                Log.note("Searching for frontier: {{frontier}} ", frontier=frontier)
                Log.note("HG URL: {{url}}", url='https://hg.mozilla.org/' + self.config.hg.branch + '/rev/' + frontier)
                while not found_last_frontier:
                    # Get a changelog
                    clog_url = 'https://hg.mozilla.org/' + self.config.hg.branch + '/json-log/' + final_rev
                    try:
                        clog_obj = http.get_json(clog_url, retry=RETRY)
                    except Exception as e:
                        Log.error("Unexpected error getting changset-log for {{url}}", url=clog_url, error=e)

                    cset = ''
                    still_looking = True
                    # For each changeset/node
                    for clog_cset in clog_obj['changesets']:
                        cset = clog_cset['node'][:12]
                        if cset == frontier:
                            still_looking = False
                            break
                        csets.append(cset)

                    if not still_looking:
                        found_last_frontier = True
                    final_rev = cset

                # No csets found means that we are already
                # at the latest revisions.
                if len(csets) == 0:
                    continue

                # Get all the latest ccov and jsdcov revisions
                if (not coverage_revisions) and only_coverage_revisions:
                    active_data_url = 'http://activedata.allizom.org/query'
                    query_json = {
                        "limit": 1000,
                        "from": "task",
                        "where": {"and": [
                            {"in": {"build.type": ["ccov", "jsdcov"]}},
                            {"gte": {"run.timestamp": {"date": "today-day"}}},
                            {"eq": {"repo.branch.name": self.config.hg.branch}}
                        ]},
                        "select": [
                            {"aggregate": "min", "value": "run.timestamp"},
                            {"aggregate": "count"}
                        ],
                        "groupby": ["repo.changeset.id12"]
                    }
                    coverage_revisions_resp = http.post_json(active_data_url, retry=RETRY, data=query_json)
                    coverage_revisions = [rev_arr[0] for rev_arr in coverage_revisions_resp.data]

                # Reverse changeset list and for each code coverage revision
                # found by going through the list from oldest to newest,
                # update _all known_ file frontiers to that revision.
                csets.reverse()
                prev_cset = frontier
                for cset in csets:
                    if please_stop:
                        return
                    if only_coverage_revisions:
                        if cset not in coverage_revisions:
                            continue
                    if DEBUG:
                        Log.note("Moving frontier {{frontier}} forward to {{cset}}.", frontier=prev_cset, cset=cset)

                    # Update files
                    self.get_tuids_from_files(files, cset, going_forward=True)

                    ran_changesets = True
                    prev_cset = cset

            if not ran_changesets:
                (please_stop | Till(seconds=DAEMON_WAIT_AT_NEWEST.seconds)).wait()

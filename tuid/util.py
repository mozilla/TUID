# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple

from jx_python import jx
from mo_files.url import URL
from mo_hg.apply import Line, SourceFile
from mo_logs import Log
from pyLibrary.sql import quote_set, sql_list
from mo_threads import Till
from mo_dots import wrap

TIMEOUT = 10
HG_URL = URL("https://hg.mozilla.org/")


class TuidLine(Line, object):
    def __init__(self, tuidmap, **kwargs):
        super(TuidLine, self).__init__(tuidmap.line, **kwargs)
        self.tuid = tuidmap.tuid

    def __str__(self):
        return "TuidLine{tuid=" + str(self.tuid) + ": line=" + str(self.line) + "}"


class AnnotateFile(SourceFile, object):
    def __init__(self, filename, lines, tuid_service=None):
        super(AnnotateFile, self).__init__(filename, lines)
        self.tuid_service = tuid_service
        self.failed_file = False

    def annotation_to_lines(self, annotation):
        self.lines = [TuidLine(tuidmap) for tuidmap in annotation]

    def lines_to_annotation(self):
        return [TuidMap(line_obj.tuid, line_obj.line) for line_obj in self.lines]

    def replace_line_with_tuidline(self):
        new_lines = []
        for line_obj in self.lines:
            if type(line_obj) == TuidLine:
                new_lines.append(line_obj)
                continue
            new_line_obj = TuidLine(
                TuidMap(None, line_obj.line), filename=line_obj.filename, is_new_line=True
            )
            new_lines.append(new_line_obj)
        self.lines = new_lines
        return self.lines

    def create_and_insert_tuids(self, revision):
        self.replace_line_with_tuidline()

        line_origins = []
        all_new_lines = []
        for line_obj in self.lines:
            line_entry = (line_obj.filename, revision, line_obj.line)
            if not line_obj.tuid or line_obj.is_new_line:
                all_new_lines.append(line_obj.line)
            line_origins.append(line_entry)

        # Get the new lines, excluding those that have existing tuids
        existing_tuids = {}
        if len(all_new_lines) > 0:
            try:
                query = {
                    "size": 10000,
                    "_source": {"includes": ["tuid", "file", "revision", "line"]},
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {"file": self.filename}},
                                {"term": {"revision": revision}},
                                {"terms": {"line": all_new_lines}},
                            ]
                        }
                    },
                }
                result = self.tuid_service.temporal.search(query)
                existing_tuids = {}

                for r in result.hits.hits:
                    s = r._source
                    existing_tuids.update({s.line: s.tuid})
            except Exception as e:
                # Log takes out important output, use print instead
                self.failed_file = True
                print("Trying to find new lines: " + str(all_new_lines))
                Log.error("Error encountered:", cause=e)

        insert_entries = []
        insert_lines = set(all_new_lines) - set(existing_tuids.keys())
        if len(insert_lines) > 0:
            try:
                insert_entries = [
                    (self.tuid_service.tuid(),) + line_origins[linenum - 1]
                    for linenum in insert_lines
                ]

                self.tuid_service._insert_max_tuid()

            except Exception as e:
                Log.note(
                    "Failed to insert new tuids (likely due to merge conflict) on {{file}}: {{cause}}",
                    file=self.filename,
                    cause=e,
                )
                self.failed_file = True
                return

        fmt_inserted_lines = {line: tuid for tuid, _, _, line in insert_entries}
        for line_obj in self.lines:
            # If a tuid already exists for this line,
            # replace, otherwise, use the newly created one.
            if line_obj.line in existing_tuids:
                line_obj.tuid = existing_tuids[line_obj.line]
            elif line_obj.line in fmt_inserted_lines:
                line_obj.tuid = fmt_inserted_lines[line_obj.line]

            if not line_obj.tuid:
                Log.warning(
                    "Cannot find TUID at {{file}} and {{rev}}for: {{line}}",
                    file=self.filename,
                    rev=revision,
                    line=line_obj,
                )
                self.failed_file = True
                return


def map_to_array(pairs):
    """
    MAP THE (tuid, line) PAIRS TO A SINGLE ARRAY OF TUIDS
    :param pairs:
    :return:
    """
    if pairs:
        pairs = [TuidMap(*p) for p in pairs]
        max_line = max(p.line for p in pairs)
        tuids = [None] * max_line
        for p in pairs:
            if p.line:  # line==0 IS A PLACEHOLDER FOR FILES THAT DO NOT EXIST
                tuids[p.line - 1] = p.tuid
        return tuids
    else:
        return None


def wait_until(index, condition):
    timeout = Till(seconds=TIMEOUT)
    while not timeout:
        if condition():
            break
        index.refresh()


def delete(index, filter):
    index.delete_record(filter)
    index.refresh()
    wait_until(index, lambda: index.search({"size": 0, "query": filter}).hits.total == 0)


def insert(index, records):
    ids = records.value._id
    index.extend(records)
    index.refresh()
    wait_until(
        index,
        lambda: index.search({"size": 0, "query": {"terms": {"_id": ids}}}).hits.total
        == len(records),
    )


# Used for increasing readability
# Can be accessed with tmap_obj.line, tmap_obj.tuid
TuidMap = namedtuple(str("TuidMap"), [str("tuid"), str("line")])
MISSING = TuidMap(-1, 0)

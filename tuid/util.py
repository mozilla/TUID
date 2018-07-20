# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from collections import namedtuple
from mo_files.url import URL
from mo_hg.apply import Line, SourceFile
from mo_logs import Log
from pyLibrary.sql import quote_set, sql_list
from pyLibrary.sql.sqlite import quote_value

HG_URL = URL('https://hg.mozilla.org/')


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

    def annotation_to_lines(self, annotation):
        self.lines = [TuidLine(tuidmap) for tuidmap in annotation]

    def lines_to_annotation(self):
        return [
            TuidMap(line_obj.tuid, line_obj.line)
            for line_obj in self.lines
        ]

    def replace_line_with_tuidline(self):
        new_lines = []
        for line_obj in self.lines:
            if type(line_obj) == TuidLine:
                new_lines.append(line_obj)
                continue
            new_line_obj = TuidLine(
                TuidMap(None, line_obj.line),
                filename=line_obj.filename,
                is_new_line=True
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

        with self.tuid_service.conn.transaction() as t:
            # Get the new lines, excluding those that have existing tuids
            existing_tuids = {}
            if len(all_new_lines) > 0:
                try:
                    existing_tuids = {
                        line: tuid
                        for tuid, file, revision, line in t.query(
                            "SELECT tuid, file, revision, line FROM temporal"
                            " WHERE file = " + quote_value(self.filename)+
                            " AND revision = " + quote_value(revision) +
                            " AND line IN " + quote_set(all_new_lines)
                        ).data
                    }
                except Exception as e:
                    Log.note("Trying to find new lines: {{newl}}", newl=str(all_new_lines))
                    Log.error("Error encountered:", cause=e)

            insert_entries = []
            insert_lines = set(all_new_lines) - set(existing_tuids.keys())
            if len(insert_lines) > 0:
                try:
                    insert_entries = [
                        (self.tuid_service.tuid(),) + line_origins[linenum-1]
                        for linenum in insert_lines
                    ]
                    t.execute(
                        "INSERT INTO temporal (tuid, file, revision, line) VALUES " +
                        sql_list(quote_set(entry) for entry in insert_entries)
                    )
                except Exception as e:
                    Log.warning("Failed to insert new tuids {{cause}}", cause=e)

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
                        line=str(line_obj)
                    )


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
                tuids[p.line-1] = p.tuid
        return tuids
    else:
        return None


# Used for increasing readability
# Can be accessed with tmap_obj.line, tmap_obj.tuid
TuidMap = namedtuple(str("TuidMap"), [str("tuid"), str("line")])
MISSING = TuidMap(-1, 0)


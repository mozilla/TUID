# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from collections import namedtuple
from mo_hg.apply import Line, SourceFile


class TuidLine(Line):

    def __init__(self, tuidmap, **kwargs):
        super(Line, self).__init__(tuidmap.line, **kwargs)
        self.tuid = tuidmap.tuid


class AnnotateFile(SourceFile):

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
            if type(line_obj, TuidLine):
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


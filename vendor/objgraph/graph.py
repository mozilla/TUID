# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from collections import defaultdict

try:
    iteritems = dict.iteritems
except AttributeError:
    # Python 3.x compatibility
    iteritems = dict.items

from pyLibrary.graphs import Graph


class MemoryGraph(Graph):
    # Graph data structure, undirected by default.
    def __init__(self, node_type):
        super(MemoryGraph, self).__init__(node_type)
        self._objs_seen = {}

    def add_objects(self, objs):
        for obj in objs:
            self._objs_seen[id(obj)] = obj

    def get_object(self, objid):
        return None if objid not in self._objs_seen else self._objs_seen[objid]

    def object_neighbours(self, obj):
        return [self.get_object(objid) for objid in self.get_children(id(obj))] if id(obj) in self.vertices() else []

    def convert_tree_to_memtree(self, tree):
        tree._objs_seen = self._objs_seen
        tree.object_neighbours = self.object_neighbours
        tree.get_object = self.get_object
        return tree

# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from collections import defaultdict


class MemoryGraph(object):
    # Graph data structure, undirected by default.
    def __init__(self, connections, directed=True):
        self._graph = defaultdict(set)
        self._directed = directed
        self._roots = []
        self._root = None
        if connections:
            self.add_connections(connections)

    def neighbours(self, node):
        return self._graph[node] if node in self._graph else None

    def vertices(self):
        return list(self._graph.keys())

    def edges(self):
        # Returns edges (v1, v2),
        # where we can travel through v1 to v2.
        edges = set()
        for node1 in self._graph:
            for node2 in self._graph[node1]:
                edges.add((node1, node2))
        return edges

    def add_connections(self, connections):
        # Add connections (list of tuple pairs) to graph

        for node1, node2 in connections:
            self.add(node1, node2)

    def add(self, node1, node2):
        # Add connection between node1 and node2 if undirected
        # or only from node1 to node2 if directed.

        self._graph[node1].add(node2)
        if not self._directed:
            self._graph[node2].add(node1)

    def remove(self, node):
        # Remove all references to node

        for n, cxns in self._graph.iteritems():
            try:
                cxns.remove(node)
            except KeyError:
                pass
        try:
            del self._graph[node]
        except KeyError:
            pass

    def is_connected(self, node1, node2):
        # Is node1 directly connected to node2?
        # Since the graph could be directed, check with node1's edges.

        return node1 in self._graph and node2 in self._graph[node1]

    def find_path(self, node1, node2, path=[]):
        # Find any path between node1 and node2 (may not be shortest)

        path = path + [node1]
        if node1 == node2:
            return path
        if node1 not in self._graph:
            return None
        for node in self._graph[node1]:
            if node not in path:
                new_path = self.find_path(node, node2, path)
                if new_path:
                    return new_path
        return None

    def is_root(self, node):
        # Checks if a node is the root of the graph

        if not self._graph[node]:
            # Leaf or root if it's the only node
            return len(self._graph) == 1

        for node1 in self._graph:
            if node == node1 or not self._graph[node1]:
                continue
            if self.is_connected(node1, node):
                # If we can go from node2 to node1 using
                # node2's edges, this is not a root.
                return False
        return True

    def find_roots(self, max_roots=None, set_graph=False):
        # Finds all roots of the graph
        #
        # All roots have no in-going edges (except for undirected
        # whose roots have 1).

        roots = []
        edges = self.edges()
        nodecounts = {node: 0 for node in self.vertices()}

        for _, in_node in edges:
            nodecounts[in_node] += 1
        for node, count in nodecounts:
            if self._directed:
                if count == 0:
                    roots.append(node)
            elif count == 1:
                roots.append(node)

        if set_graph:
            self._roots = roots
            if self._roots and len(self._roots) == 1:
                self._root = self._roots[0]
        else:
            return roots

    def _dfs_spanning_tree(self, root=None):
        if not root and not self._root:
            self.find_roots(set_graph=True)
            if not self._root:
                print("Graph has no root, set one manually.")
                return None
        else:
            self._root = root

        visited = set()
        dfs_graph = MemoryGraph(None, directed=True)
        return self._dfs_search(self._root, dfs_graph, visited)

    def _dfs_search(self, root, dfs_graph, visited):
        visited.add(root)
        for neighbour in self._graph[root]:
            if neighbour in visited:
                continue
            self._dfs_search(neighbour, dfs_graph, visited)
            dfs_graph.add(root, neighbour)
        return dfs_graph

    def _dominator_graph(self):
        # See http://www.cs.au.dk/~gerth/advising/thesis/henrik-knakkegaard-christensen.pdf
        # Section 3.2 Vertex Removal Algorithm for a description of
        # how this works.

        dominator_tree = MemoryGraph(None, directed=True)
        vertices = self.vertices()

        for v1 in vertices:
            notvisited = set(self.vertices())
            visited = set()
            for v2 in notvisited:
                if self.find_path(v1, v2):
                    visited.add(v2)

            dominating = notvisited - visited
            if dominating:
                dominator_tree.add_connections([
                    (v1, dom)
                    for dom in dominating
                ])

        return dominator_tree

    def __str__(self):
        # Prints out {Node1: {Nodes that we can travel to directly through Node1}}
        return '{}({})'.format(self.__class__.__name__, dict(self._graph))
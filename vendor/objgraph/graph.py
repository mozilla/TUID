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

class MemoryGraph(object):
    # Graph data structure, undirected by default.
    def __init__(self, connections, directed=True):
        self._graph = defaultdict(set)
        self._directed = directed
        self._roots = []
        self._objs_seen = {}
        self.root = None
        if connections:
            self.add_connections(connections)

    def neighbours(self, node):
        return self._graph[node] if node in self._graph else []

    def object_neighbours(self, obj):
        return [self.get_object(objid) for objid in self._graph[id(obj)]] if id(obj) in self._graph else []

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
        if node2 not in self._graph:
            self._graph[node2] = set()
        if not self._directed:
            self._graph[node2].add(node1)

    def add_objects(self, objs):
        for obj in objs:
            self._objs_seen[id(obj)] = obj

    def get_object(self, objid):
        return None if objid not in self._objs_seen else self._objs_seen[objid]

    def remove(self, node):
        # Remove all references to node

        for n, cxns in iteritems(self._graph):
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
        # whose "roots" have 1).

        roots = []
        edges = self.edges()
        nodecounts = {node: 0 for node in self.vertices()}
        print(self.vertices())
        for o, in_node in edges:
            nodecounts[in_node] += 1
        for node, count in iteritems(nodecounts):
            if self._directed and count == 0:
                roots.append(node)
            elif count == 1:
                roots.append(node)
            if max_roots and len(roots) >= max_roots:
                break

        if set_graph:
            self._roots = roots
            if self._roots and len(self._roots) == 1:
                self.root = self._roots[0]
        return roots

    def _dfs_spanning_tree(self, root=None):
        if not root and not self.root:
            self.find_roots(set_graph=True)
            if not self.root:
                print("Graph has no root, set one manually.")
                return None
        else:
            self.root = root

        visited = set()
        dfs_graph = MemoryGraph(None, directed=True)
        return self._dfs_search(self.root, dfs_graph, visited)

    def _dfs_search(self, root, dfs_graph, visited):
        visited.add(root)
        for neighbour in self._graph[root]:
            if neighbour in visited:
                continue
            self._dfs_search(neighbour, dfs_graph, visited)
            dfs_graph.add(root, neighbour)
        return dfs_graph

    def fully_connect_roots(self):
        self.find_roots(set_graph=True)
        for root1 in self._roots:
            for root2 in self._roots:
                if root1 == root2:
                    continue
                self.add(root1, root2)
                if self._directed:
                    self.add(root2, root1)
        return self._roots

    def traverse_tree(self):
        # Returns (notvisited, visited)
        visited = set()
        notvisited = self.vertices()
        queue = [self.root]
        if self.root not in self._graph:
            return notvisited, visited

        ignore = set()
        ignore.add(self.root)
        while queue:
            node = queue.pop()
            print('here')
            neighbours = self._graph[node]
            for neighbour in neighbours:
                if neighbour in ignore:
                    continue
                visited.add(neighbour)
                if neighbour in notvisited:
                    notvisited.remove(neighbour)
                    queue.append(neighbour)
                ignore.add(neighbour)

        return notvisited, visited


    def dominator_tree(self):
        # See http://www.cs.au.dk/~gerth/advising/thesis/henrik-knakkegaard-christensen.pdf
        # Section 3.2 Vertex Removal Algorithm for a description of
        # how this works.
        dominator_trees = []
        vertices = self.vertices()
        self.find_roots(set_graph=True)
        if not self.root and not self._roots:
            print("No roots found - picking the first vertex found.")
            self._roots = [vertices[0]]
        if len(self._roots) > 1:
            print("Multiple roots found, returning dominator for each.")

        for root in self._roots:
            dominator_tree = MemoryGraph(None, directed=True)
            dominator_tree._objs_seen = self._objs_seen
            dominator_tree.root = root
            print(len(vertices))
            for v1 in vertices:
                tmp_tree = MemoryGraph(self.edges(), directed=True)
                tmp_tree.root = self.root
                tmp_tree.remove(v1)
                notvisited, visited = tmp_tree.traverse_tree()

                dominating = set(notvisited) - set(visited)
                if dominating:
                    # Add directed paths: dominator -> dominatee
                    dominator_tree.add_connections([
                        (v1, dom)
                        for dom in dominating
                    ])
            dominator_trees.append(dominator_tree)
        return dominator_trees

    def __str__(self):
        # Prints out {Node1: {Nodes that we can travel to directly through Node1}}
        return '{}({})'.format(self.__class__.__name__, dict(self._graph))
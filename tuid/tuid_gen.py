from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


from mo_dots import wrap, coalesce, set_default
from mo_logs import Log, constants, startup
from mo_logs.exceptions import suppress_exception
from tuid.util import insert
from pyLibrary.env import elasticsearch
import os


TEMPORAL_SCHEMA = {
    "settings": {"index.number_of_replicas": 1, "index.number_of_shards": 1},
    "mappings": {
        "temporaltype": {
            "_all": {"enabled": False},
            "properties": {
                "tuid": {"type": "integer", "store": True},
                "revision": {"type": "keyword", "store": True},
                "file": {"type": "keyword", "store": True},
                "line": {"type": "integer", "store": True},
            },
        }
    },
}


class TUID_Generator:
    def __init__(self):

        config = startup.read_settings(filename=os.environ.get("TUID_CONFIG"))

        constants.set(config.constants)
        Log.start(config.debug)

        self.esconfig = config.tuid.esservice
        self.es_temporal = elasticsearch.Cluster(kwargs=self.esconfig.temporal)
        self.es_annotations = elasticsearch.Cluster(kwargs=self.esconfig.annotations)

        self.init_db()
        # TODO: Do the configuration directly from config file
        # Not from service call
        # self.service = TUIDService(config.tuid)

        query = {"size": 0, "aggs": {"value": {"max": {"field": "tuid"}}}}
        self.next_tuid = int(
            coalesce(eval(str(self.temporal.search(query).aggregations.value.value)), 0) + 1
        )

    def init_db(self):
        temporal = self.esconfig.temporal
        set_default(temporal, {"schema": TEMPORAL_SCHEMA})
        self.temporal = self.es_temporal.get_or_create_index(kwargs=temporal)
        self.temporal.refresh()

        total = self.temporal.search({"size": 0})
        while not total.hits:
            total = self.temporal.search({"size": 0})
        with suppress_exception:
            self.temporal.add_alias()

    def tuid(self):
        """
        :return: next tuid
        """
        try:
            return self.next_tuid
        finally:
            self.next_tuid += 1

    def get_tuid_tobe_assigned(self):
        query = {
            "_source": {"includes": ["revision", "line", "file", "tuid"]},
            "query": {"bool": {"must": [{"term": {"tuid": 0}}]}},
            "size": 100,
        }
        temp = self.temporal.search(query).hits.hits
        return temp

    def _make_record_temporal(self, line):
        id = line._id
        record = line._source
        record["_id"] = id
        record["tuid"] = self.tuid()
        return wrap([{"value": record}])


gen = TUID_Generator()

while True:
    line = gen.get_tuid_tobe_assigned()
    # {'file': 'gfx/thebes/GLContextProviderGLX.cpp', 'line': 1, 'tuid': 0, 'revision': '0ec22e77aefc'}
    for l in line:
        record = gen._make_record_temporal(l)
        insert(gen.temporal, record)
        # print("Giving tuid for "+str(record))

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


from mo_dots import wrap, coalesce
from mo_logs import Log, constants, startup, Except
from tuid.service import TUIDService
from tuid.util import insert

OVERVIEW = None
QUERY_SIZE_LIMIT = 10 * 1000 * 1000
EXPECTING_QUERY = b"expecting query\r\n"
TOO_BUSY = 10
TOO_MANY_THREADS = 4


class TUID_Generator:
    def __init__(self):

        config = startup.read_settings(
            filename="/home/ajupazhamayil/TUID/tests/travis/config.json"
        )
        constants.set(config.constants)
        Log.start(config.debug)

        # TODO: Do the configuration directly from config file
        # Not from service call
        self.service = TUIDService(config.tuid)

        query = {"size": 0, "aggs": {"value": {"max": {"field": "tuid"}}}}
        self.next_tuid = int(
            coalesce(eval(str(self.service.temporal.search(query).aggregations.value.value)), 0)
            + 1
        )

    def tuid(self):
        """
        :return: next tuid
        """
        try:
            return self.next_tuid
        finally:
            self.next_tuid += 1

    def get_tuid_tobe_assigned(self):
        # Returns a single TUID if it exists else None
        query = {
            "_source": {"includes": ["revision", "line", "file", "tuid"]},
            "query": {"bool": {"must": [{"term": {"tuid": 0}}]}},
            "size": 100,
        }
        temp = self.service.temporal.search(query).hits.hits
        return temp

    def _make_record_temporal(self, line):
        id = line._id
        record = line._source
        record["_id"] = id
        record["tuid"] = self.tuid()
        # record = {
        #     "_id": revision + file + str(line),
        #     "tuid": tuid,
        #     "revision": revision,
        #     "file": file,
        #     "line": line,
        # }
        return wrap([{"value": record}])


gen = TUID_Generator()

while True:
    line = gen.get_tuid_tobe_assigned()
    # {'file': 'gfx/thebes/GLContextProviderGLX.cpp', 'line': 1, 'tuid': 0, 'revision': '0ec22e77aefc'}
    for l in line:
        record = gen._make_record_temporal(l)
        insert(gen.service.temporal, record)
        # print("Giving tuid for "+str(record))

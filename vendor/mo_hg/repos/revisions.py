# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

from mo_dots import Data


class Revision(Data):
    def __hash__(self):
        return hash((self.branch.name.lower(), self.changeset.id[:12]))

    def __eq__(self, other):
        if other == None:
            return False
        return (self.branch.name.lower(), self.changeset.id[:12]) == (other.branch.name.lower(), other.changeset.id[:12])


revision_schema = {


    "settings": {
        "index.number_of_replicas": 1,
        "index.number_of_shards": 6,
        "analysis": {
            "analyzer": {
                "description_limit": {
                    "type": "custom",
                    "tokenizer": "keyword",
                    "filter": [
                        "lowercase",
                        "asciifolding",
                        {
                            "type": "limit",
                            "max": 100,
                            "min": 5
                        }
                    ]
                }
            }
        }
    },
    "mappings": {
        "revision": {
            "_source": {
                "compress": False
            },
            "_id": {
                "index": "not_analyzed",
                "type": "string",
                "store": True
            },
            "_all": {
                "enabled": False
            },
            "_routing": {
                "required": True,
                "path": "changeset.id12"
            },
            "dynamic_templates": [
                {
                    "default_strings": {
                        "mapping": {
                            "index": "not_analyzed",
                            "type": "string",
                            "doc_values": True
                        },
                        "match_mapping_type": "string",
                        "match": "*"
                    }
                },
                {
                    "default_longs": {
                        "mapping": {
                            "index": "not_analyzed",
                            "type": "long",
                            "doc_values": True
                        },
                        "match_mapping_type": "long",
                        "match": "*"
                    }
                },
                {
                    "default_integers": {
                        "mapping": {
                            "index": "not_analyzed",
                            "type": "long",
                            "doc_values": True
                        },
                        "match_mapping_type": "integer",
                        "match": "*"
                    }
                }
            ],
            "properties": {
                "changeset": {
                    "type": "object",
                    "properties": {
                        "description": {
                            "index": "analyzed",
                            "type": "string",
                            "fields": {
                                "raw": {
                                    "type": "string",
                                    "analyzer": "description_limit"
                                }
                            }
                        },
                        "diff": {
                            "type": "nested",
                            "dynamic": True,
                            "properties": {
                                "changes": {
                                    "type": "nested",
                                    "dynamic": True,
                                    "properties": {
                                        "new": {
                                            "type": "object",
                                            "dynamic": True,
                                            "properties": {
                                                "content": {
                                                    "type": "string",
                                                    "index": "no"
                                                }
                                            }
                                        },
                                        "old": {
                                            "type": "object",
                                            "dynamic": True,
                                            "properties": {
                                                "content": {
                                                    "type": "string",
                                                    "index": "no"
                                                }
                                            }
                                        }
                                    }
                                }

                            }
                        }
                    }
                }
            }
        }
    }
}

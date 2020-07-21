# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/7/21

from typing import List

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from SmartAI.database import Question


class ESConfig(object):
    doc_type = "default"
    es = Elasticsearch(["localhost:9200"])


class ESMinxin(object):
    def ping(self):
        return self.es.ping()

    def info(self):
        return self.es.info()


class ESCore(ESConfig, ESMinxin):

    model: "Base" = None

    def __init__(self, index: str):
        self.index = index

    def insert(self, obj: dict):
         self.es.index(
              index=self.index,
              doc_type=self.doc_type,
              body=obj,
          )
        return True

    def create(self, objs: List[dict]):
        actions = []
        for obj in objs:
            actions.append(
                dict(
                    _index=self.index,
                    _type=self.doc_type,
                    _source=obj,
                )
            )
        helpers.bulk(self.es, actions)
        return True


    def search(self, keyword, size=5):
        dsl = {
            "query": {
                "match": {
                    "question": keyword,
                }
            }
        }
        rets = self.es.search(
            index=self.index, body=dsl, size=size,
            doc_type=self.doc_type,
            filter_path=["hits.hits._source"],
        )
        hits = [obj["_source"] for obj in rets.get("hits", {}).get("hits", [])]
        return hits

    def execute_delete(self, query):
        self.es.delete_by_query(
                index=self.index,
                body={"query":query},
                doc_type=self.doc_type,
        )
        return True


    def clear(self):
        return self.execute_delete({"match_all": {}})

    def delete_question_by_id(self, id):
        return self.execute_delete({"match": {"id": id}})

    def delete_index(self):
        self.es.indices.delete(self.index)

    delete = delete_index

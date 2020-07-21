# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/7/20

import json

from py2neo import NodeMatcher
from py2neo import Graph, Node, Relationship

g = Graph("bolt://localhost:7687", password="neo")
matcher = NodeMatcher(g)


if __name__ == '__main__':

    def load_json(path):
        with open(path, encoding="utf-8") as file:
            data = json.load(file)
        return data

    records = load_json("data/dataset/movies.json")

    movies = set()
    actors = set()


    history = {}
    mapping = {
        "name": "电影名称",
        "to_url": "百度链接",
        "actors": "演员名称",
    }
    for movie, detail in records.items():
        for k, v in detail.items():
            k = mapping.get(k, k)
            if isinstance(v, list):
                v = list(filter(lambda obj: obj.strip(), v))
                history.setdefault(k, set()).update(set(v))
            else:
                if v.strip():
                    history.setdefault(k, set()).add(v)

    tx = g.begin()
    for label, aset in history.items():
        for element in aset:
            node = Node(label, **{"label": element})
            tx.create(node)
    tx.commit()

    tx = g.begin()
    has_seen = set()
    for movie, detail in records.items():
        movie_nodes = matcher.match(**{"label": movie})
        for movie_node in movie_nodes:
            for k, v in detail.items():
                if k == "name":
                    continue
                if isinstance(v, list):
                    for _v in v:
                        if (movie, mapping.get(k, k), _v) in has_seen:
                            continue
                        else:
                            has_seen.add((movie, mapping.get(k, k), _v))
                        nodes = matcher.match(**{"label": _v})
                        for node in nodes:
                            relation = Relationship(movie_node, mapping.get(k, k), node)
                            tx.create(relation)
                else:
                    if (movie, mapping.get(k, k), v) in has_seen:
                        continue
                    else:
                        has_seen.add((movie, mapping.get(k, k), v))
                    nodes = matcher.match(**{"label": v})
                    for node in nodes:
                        relation = Relationship(movie_node, mapping.get(k, k), node)
                        tx.create(relation)
    tx.commit()

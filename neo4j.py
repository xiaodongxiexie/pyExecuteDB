# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/4/24

from time import perf_counter
from typing import List, Dict, Any, NoReturn

from logging import Logger

from py2neo import Graph
from py2neo import Node, Relationship

logger = Logger("neo4j")


def fetch_data_from_neo4j(host, user, password):
    graph = Graph(host=host, auth=(user, password))
    return graph


class BuildGraph:
    graph = fetch_data_from_neo4j

    def __init__(self, label_name: str = "Your-label"):
        self.tx = None
        self.label_name = label_name
        self._count = 0
        self._start_time = None
        self._end_time = None

    def start(self) -> NoReturn:
        self._start_time = perf_counter()
        self.tx = self.graph.begin()

    def _process(self,
                 root_node: Node,
                 info: dict) -> NoReturn:
        pass


    def end(self) -> NoReturn:
        self._end_time = perf_counter()
        self.tx.commit()


    def process(self, info: dict) -> NoReturn:
        _root_node = Node(self.label_name, name=info["name"])
        self._process(_root_node,
                      info,)
        self._count += 1

    def stats(self) -> NoReturn:
        """doc
        执行入库操作耗时，入库节点个数等信息统计
        """
        logger.info("create master root node %s", self._count)
        on = self._end_time and self._start_time
        if on:
            logger.info("write to neo4j cost %s seconds", self._end_time-self._start_time)

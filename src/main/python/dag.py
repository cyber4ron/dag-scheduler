# -*- coding:utf-8 -*-

import json
import threading
import re
import config
import pprint
import sys

from utils import logger
from task import Task, SinkTask


class DAG(object):
    DEP = 0
    DOWN = 1

    def __init__(self):
        # 图build后，对图的修改需要sync
        self.lock = threading.Lock()
        self.task_types = set()
        self.task_idx = {}  # key is task.id, value is task object
        self.graph = {}

    def add_node(self, task):
        self.task_idx[task.id] = task
        self.graph[task.id] = [set(), set()]  # deps_ids, downstream_ids

    # return the tasks that task_target depends on
    def dep_match(self, task_target):
        matches = []
        for task in self.task_idx.values():
            if task_target.dep_match(task):
                matches.append(task.id)
        return matches

    def resolve_deps(self):
        for task in self.task_idx.values():
            for dep_id in self.dep_match(task):
                self.graph[task.id][DAG.DEP].add(dep_id)
                self.graph[dep_id][DAG.DOWN].add(task.id)

    def add_sink(self):
        sink = SinkTask()
        self.add_node(sink)
        for id, edges in self.graph.items():
            if id != sink.id and len(edges[DAG.DOWN]) == 0:  # 没有后继
                self.graph[sink.id][DAG.DEP].add(id)
                edges[DAG.DOWN].add(sink.id)

    def build_dag(self, json_path):
        tasks_json = json.loads(open(json_path).read())

        for task_type in tasks_json["concurrency"].keys():
            self.task_types.add(task_type.decode("utf-8").encode("ascii"))

        for task_json in tasks_json["tasks"]:
            task = Task(task_json)
            self.add_node(task)

        self.resolve_deps()

        self.add_sink()

        self.log_graph()

    def log_graph(self):
        from StringIO import StringIO
        import pprint

        old_stdout = sys.stdout
        sys.stdout = temp_stdout = StringIO()

        # for task in sorted(self.task_idx.values()): print task
        # logger.info("===========> graph idx: \n%s", temp_stdout.getvalue())

        pprint.pprint(self.graph)
        logger.info("===========> graph structure: \n%s", temp_stdout.getvalue())

        sys.stdout = old_stdout

    def dump_graph(self):
        tasks = []
        [tasks.append(x.to_dict()) for x in self.task_idx.values() if x.id != "Sink"]
        with open(config.json_dump_path, 'w') as dump_path:
            json.dump({"tasks": tasks}, dump_path, indent=4)

        # check failed task
        err = 0
        for t in tasks:
            if t.get("status") == "FINISH_FAILED":
                logger.error("===========> task failed: %s" % str(t))
                err += 1
        return err




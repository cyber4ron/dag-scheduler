# -*- coding:utf-8 -*-

from dag import DAG
from utils import logger, send_mail
from config import TaskStatus

from local_executor import LocalExecutor
from remote_executor import RemoteExecutor
from hive_executor import HiveExecutor
from distributed_executor import DistributedExecutor


class Processor(object):
    def __init__(self, dag):
        # 对图的修改需要sync
        self.dag = dag
        self.executors = {}  # key is task type, value is one Executor Object for this type
        self.concurrency = {}  # key is task type, value is total concurrency

    def init_executors(self):
        self.executors[DistributedExecutor.executor_type] = DistributedExecutor(self.task_done)
        self.executors[HiveExecutor.executor_type] = HiveExecutor(self.task_done)
        self.executors[LocalExecutor.executor_type] = LocalExecutor(self.task_done)
        self.executors[RemoteExecutor.executor_type] = RemoteExecutor(self.task_done)

    def start_executors(self):
        for executor in self.executors.values():
            executor.start()
        for executor in self.executors.values():
            executor.join()

    def stop_executors(self):
        for executor in self.executors.values():
            executor.clear_queue()
            executor.stop()

    def is_dep_finish_succ(self, task_id):
        return len(self.dag.graph[task_id][DAG.DEP]) == 0 or \
               reduce(lambda x, y: x and y, map(lambda x: self.dag.task_idx[x].status == TaskStatus.FINISH_SUCC,
                                                self.dag.graph[task_id][DAG.DEP]))

    def is_task_not_done(self, task):
        return task.status == TaskStatus.CREATED or task.status == TaskStatus.FINISH_FAILED

    def start(self):
        self.init_executors()
        with self.dag.lock:
            for task_id, task in sorted(self.dag.task_idx.items()):
                if self.is_task_not_done(task) and self.is_dep_finish_succ(task_id):
                    if task.is_sink: return  # config error
                    task.status = TaskStatus.RUNNING
                    self.executors[task.type].add_task(task)
        self.start_executors()

    def task_done(self, task_id, rc):
        with self.dag.lock:
            if rc != 0:  # task failed
                self.dag.task_idx[task_id].status = TaskStatus.FINISH_FAILED
                logger.info("task failed, task_id=: %s", task_id)
                send_mail("task failed", "task_id=" + task_id)
                self.stop_executors()
                return

            # task finish
            self.dag.task_idx[task_id].status = TaskStatus.FINISH_SUCC
            logger.info("%s done.", task_id)
            for down_id in self.dag.graph[task_id][DAG.DOWN]:
                deps_done = self.is_dep_finish_succ(down_id)
                if deps_done:
                    if self.dag.task_idx[down_id].is_sink:
                        self.stop_executors()
                    else:
                        self.executors[self.dag.task_idx[down_id].type].add_task(
                            self.dag.task_idx[down_id])

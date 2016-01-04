# -*- coding:utf-8 -*-

import Queue
import threading
import subprocess
import config
import json

from task import SinkTask
from utils import logger


class Executor(object):
    executor_type = ""

    def __init__(self, callback):
        # to override
        self.task_done_cb = callback
        self.task_queue = Queue.Queue()
        self.consumers = []
        # to override
        self.concurrency = 1

        json_config = json.loads(open(config.json_path).read())
        self.remotes = []
        for host in json_config["remotes"]:
            self.remotes.append(host.decode("utf-8").encode("ascii"))

    # to override
    def run(self, task_obj, thread_id):
        return 1

    def do_task(self, thread_id):
        while True:
            task_obj = self.task_queue.get(block=True)
            logger.info("get from queue: %s - %s", task_obj.id, task_obj.cmd)
            # handle poison pill
            if task_obj.cmd == config.stop_cmd:
                self.add_task(SinkTask())
                logger.info("thread %s exiting...", threading.current_thread())
                break
            # run cmd
            rc = self.run(task_obj, thread_id)
            # callback
            self.task_done_cb(task_obj.id, rc)

    def add_task(self, task_obj):
        logger.info("putting to queue: %s - %s", task_obj.id, task_obj.cmd)
        self.task_queue.put(task_obj)

    def start(self):
        for i in range(self.concurrency):
            t = threading.Thread(target=self.do_task, args=(i,))
            t.daemon = True
            t.start()
            self.consumers.append(t)

    def join(self):
        [x.join() for x in self.consumers]

    def clear_queue(self):
        while not self.task_queue.empty():
            try:
                self.task_queue.get_nowait()
            except Queue.Empty:
                pass

    def stop(self):
        # poison pill
        self.add_task(SinkTask())

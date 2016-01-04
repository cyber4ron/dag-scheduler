# -*- coding:utf-8 -*-

import subprocess
import config
import json
from utils import logger
from executor import Executor

# 传给脚本(sh/py)：remote list
class DistributedExecutor(Executor):
    executor_type = "Distributed"

    def __init__(self, callback):
        super(DistributedExecutor, self).__init__(callback)
        # read concurrency
        config_json = json.loads(open(config.json_path).read())
        self.concurrency = int(config_json["concurrency"][DistributedExecutor.executor_type]["total_concurrency"])

    def run(self, task_obj, thread_id):
        cmd = "%s %s > %s 2>&1" % (task_obj.cmd, ','.join(self.remotes), task_obj.log)
        logger.info("===========> running: %s", cmd)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        process.wait()
        return process.returncode

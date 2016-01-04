# -*- coding:utf-8 -*-

import subprocess
import config
import json
from executor import Executor
from utils import logger


class HiveExecutor(Executor):
    executor_type = "Hive"

    def __init__(self, callback):
        super(HiveExecutor, self).__init__(callback)
        # read concurrency
        config_json = json.loads(open(config.json_path).read())
        self.concurrency = int(config_json["concurrency"][HiveExecutor.executor_type]["total_concurrency"])

    def run(self, task_obj, thread_id):
        cmd = "%s > %s 2>&1" % (task_obj.cmd, task_obj.log)
        logger.info("===========> running: %s", cmd)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        process.wait()
        return process.returncode

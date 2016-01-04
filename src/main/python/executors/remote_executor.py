# -*- coding:utf-8 -*-

import subprocess
import config
import json
from utils import logger
from executor import Executor

# 远程执行
class RemoteExecutor(Executor):
    executor_type = "Remote"

    def __init__(self, callback):
        super(RemoteExecutor, self).__init__(callback)
        # read concurrency
        config_json = json.loads(open(config.json_path).read())
        self.concurrency = len(config_json["remotes"]) * int(config_json["concurrency"][RemoteExecutor.executor_type]["remote_concurrency"])

    def run(self, task_obj, thread_id):
        ssh_cmd = "ssh -t -t {0} '{1} > {2} 2>&1'".format(self.remotes[thread_id % len(self.remotes)], task_obj.cmd, task_obj.log)
        logger.info("===========> running: %s", ssh_cmd)
        process = subprocess.Popen(ssh_cmd, shell=True, stdout=subprocess.PIPE)
        process.wait()
        return process.returncode

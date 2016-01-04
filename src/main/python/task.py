# -*- coding:utf-8 -*-

from exceptions import Exception
import json
import re

from utils import logger, unc_to_asc, send_mail
import config
from config import TaskStatus


class Task(object):
    def __init__(self, task_json_obj):
        # must-have fields
        self.id = self.get_field(task_json_obj, "id", True)
        self.type = self.get_field(task_json_obj, "type", True)

        # optinal fields
        self.cmd = self.get_field(task_json_obj, "cmd", False, "")
        self.log = self.get_field(task_json_obj, "log", False, "")
        self.type_params = self.get_field(task_json_obj, "type", False, "")
        self.dep_matcher = self.get_field(task_json_obj, "dep_matcher", False, "")

        # default fields
        self.is_sink = False
        self.status = TaskStatus.CREATED

    def get_field(self, task_json_obj, field_name, is_must_have, default_value=None):
        if is_must_have:
            if field_name in task_json_obj:
                return unc_to_asc(task_json_obj[field_name])
            else:
                raise Exception("task has no %s." % field_name)
        else:
            return unc_to_asc(task_json_obj[field_name]) \
                if field_name in task_json_obj else default_value


    # return: is self depend on other_task ?
    def dep_match(self, other_task):
        if other_task.id == self.id or self.dep_matcher == "": return False
        conditions = self.dep_matcher.split('|')
        is_match = False
        for cond in conditions:
            if cond[0:2] == config.le:
                # extract type
                match_obj = re.match('\w+\((.*)[0-9]{8}\)\w*', cond)
                task_type = match_obj.group(1)
                if other_task.type != task_type:
                    continue
                # extract id
                match_obj = re.match('\w+\((.*)\)\w*', cond)
                task_id_to_compare = match_obj.group(1)
                if other_task.id <= task_id_to_compare:
                    is_match = True
                    break
            elif cond[0:2] == config.eq:
                # extract task_id
                match_obj = re.match('\w+\((.*)\)\w*', cond)
                task_id = match_obj.group(1)
                if other_task.id == task_id:
                    is_match = True
                    break
        return is_match

    def __str__(self):
        return ", ".join([self.id, '\"' + self.cmd + '\"', self.status, self.type,
                          '\"' + self.dep_matcher + '\"', "is_sink=" + str(self.is_sink)])

    def to_dict(self):
        kvs = {"id": self.id,
               "type": self.type,
               "cmd": self.cmd,
               "status": self.status,
               "dep_matcher": self.dep_matcher}
        return kvs


class SinkTask(Task):
    def __init__(self):
        self.id = "Sink"
        self.is_sink = True
        self.cmd = config.stop_cmd
        self.status = TaskStatus.CREATED

    def __str__(self):
        return ", ".join([self.id, '\"' + self.cmd + '\"', "is_sink=" + str(self.is_sink), self.status])

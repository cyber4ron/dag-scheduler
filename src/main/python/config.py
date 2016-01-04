# -*- coding:utf-8 -*-

# 每次调用main时传入path
json_path = ""
json_dump_path = ""

stop_cmd = "stop."
le = "LE"
eq = "EQ"
sep = '005'


def enum(**enums):
    return type('Enum', (), enums)


# state transfer: CREATED -> RUNNING; RUNNING -> FINISH_SUCC; RUNNING -> FINISH_FAILED; FINISH_FAILED -> RUNNING
TaskStatus = enum(CREATED="CREATED",
                  RUNNING="RUNNING",
                  FINISH_SUCC='FINISH_SUCC',
                  FINISH_FAILED="FINISH_FAILED")

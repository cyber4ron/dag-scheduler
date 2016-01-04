# -*- coding:utf-8 -*-

import sys
from datetime import date, timedelta

tasks_tmpl = """
	{"id" : "Task1%date", "cmd" : "cd %bin_dir && nohup bash task1.sh %date", "log" : "%log_dir/log_task1_%date", "type":"Local", "dep_matcher":""},
	{"id" : "Task2%date", "cmd" : "cd %bin_dir && nohup bash task2.sh %date", "log" : "%log_dir/log_task2_%date", "type":"Distributed", "dep_matcher":""},
    {"id" : "Task3%date", "cmd" : "cd %py_dir && nohup python task3.py %date", "log" : "%log_dir/log_task3_%date", "type":"Hive", "dep_matcher":"EQ(Task1%date)|Task2%date"},
"""

config_tmpl = """{"tasks" : [
    %tasks
],

"concurrency" : {
    "Remote" : {"remote_concurrency":"1"},
    "Distributed" : {"total_concurrency":"1"},
    "Local" : {"total_concurrency":"8"},
    "Hive" : {"total_concurrency":"24"}
},


"remotes" : ["work@server1.hy01", "work@server2.hy01", "work@server3.hy01", "work@server4.hy01"],

"!!!comments!!!" : "命令不能是后台命令"
}

"""

# date range generator, in range [start_date, end_date)
def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n - 1), start_date + timedelta(n)


if __name__ == "__main__":
    date_from_str = sys.argv[1]
    date_to_str = sys.argv[2]

    tasks_json = ""

    for one_day_before, date in date_range(
            date(int(date_from_str[0:4]), int(date_from_str[4:6]), int(date_from_str[6:8])),
            date(int(date_to_str[0:4]), int(date_to_str[4:6]), int(date_to_str[6:8]))):
        one_day_before_str = one_day_before.strftime("%Y%m%d")
        date_str = date.strftime("%Y%m%d")

        # replace strings
        tasks_json_date = tasks_tmpl.replace("%bin_dir", "proj-package/bin"). \
            replace("%py_dir", "proj-package/python"). \
            replace("%log_dir", "log/log-%date"). \
            replace("%date", date_str). \
            replace("%yesterday", one_day_before_str)

        tasks_json += tasks_json_date

    tasks_json = tasks_json[:tasks_json.rfind(',')]  # remove tail comma
    config_json = config_tmpl.replace("%tasks", tasks_json)

    # write to file
    file_tasks_config_json = open("backtrace_tasks_{0}_{1}.json".format(date_from_str, date_to_str), 'w')
    file_tasks_config_json.write(config_json)
    file_tasks_config_json.flush()
    file_tasks_config_json.close()

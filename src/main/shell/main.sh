#!/bin/bash

source /etc/profile
source ~/.bash_profile
source commons.sh
source log.sh
source mail.sh
source execute.sh

function usage() {
echo        ""
echo        "   Usage: ./main_backtrace.sh <date_from> <date_to>"
echo        "   date format e.g.: 20140101"
echo        ""
}

if [ $# -ne 2 ]; then
    usage
    exit 1
fi

date_from=$1
date_to=$2

LOG_INFO "[================main_backtrace ${date_from} ${date_to}] start"

EXECUTE_CHECK "python ${SCHEDULER_SCRIPTS_DIR}backtrace_config_gen.py ${date_from} ${date_to}"

EXECUTE_CHECK "python ${SCHEDULER_SCRIPTS_DIR}main.py -c backtrace_tasks_${date_from}_${date_to}.json"

LOG_INFO "[================main_backtrace ${date_from} ${date_to}] completed"

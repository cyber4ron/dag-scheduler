# -*- coding:utf-8 -*-

import sys

from dag import DAG
from processor import Processor
import config
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-c", "--config_path")

if __name__ == "__main__":
    (options, args) = parser.parse_args()

    config.json_path = options.config_path
    config.json_dump_path = config.json_path + "_dump"

    dag = DAG()
    dag.build_dag(config.json_path)

    proc = Processor(dag)
    proc.start()

    err = dag.dump_graph()
    sys.exit(err)


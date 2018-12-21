# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 参数初始化
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
import argparse
import getopt

import sys

from utils.biz_excpetion import BizExcption

init_title = ["org", "sourceDataMode", "sourceDbName", "sourceTableName",
              "schemaIDdbName", "tableName",
              "savaMode", "dataDate", "dateRange", "orgPos",
              "clusterCol", "bucketsNum"]


def init():
    # 参数解析
    parser = argparse.ArgumentParser(description="数据归档组件")

    parser.add_argument("-obj", required=True, help="数据对象名")
    parser.add_argument("-org",required=True, help="机构")
    parser.add_argument("-sourceDataMode", required=True, help="机构")
    parser.add_argument("-sourceDbName", required=True, help="机构")
    parser.add_argument("-sourceTableName", required=True, help="机构")
    parser.add_argument("-filterSql", required=True, help="机构")
    parser.add_argument("-filterCol", required=True, help="机构")
    parser.add_argument("-schemaID", required=True, help="机构")
    parser.add_argument("-dbName", required=True, help="机构")
    parser.add_argument("-tableName", required=True, help="机构")
    parser.add_argument("-savaMode", required=True, help="机构")
    parser.add_argument("-dataDate", required=True, help="机构")
    parser.add_argument("-dateRange", required=True, help="机构")
    parser.add_argument("-orgPos", required=True, help="机构")
    parser.add_argument("-clusterCol", required=True, help="机构")
    parser.add_argument("-bucketsNum", required=True, help="机构")
    parser.add_argument("-allTableName", required=True, help="机构")
    parser.add_argument("-addTableName", required=True, help="机构")



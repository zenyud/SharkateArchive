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
    parser.add_argument("-sourceDataMode", required=True,
                        help="源数据增全量（1-全量 2-增量）")
    parser.add_argument("-sourceDbName", required=True, help="源库名")
    parser.add_argument("-sourceTableName", required=True, help="源表名")
    parser.add_argument("-filterSql", required=False,
                        help="采集过滤SQL条件（WHERE 后面部分）")
    parser.add_argument("-filterCol", required=False, help="过滤字段")
    parser.add_argument("-schemaID", required=True, help="取连接信息")
    parser.add_argument("-dbName", required=True, help="归档库名")
    parser.add_argument("-tableName", required=True, help="归档表名")
    parser.add_argument("-saveMode", required=True,
                        help="归档方式（1-历史全量、2-历史增量、"
                             "4-拉链、5-最近一天增量、6-最近一天全量）")
    parser.add_argument("-dataDate", required=True,
                        help="数据日期（yyyymmdd）")
    parser.add_argument("-dateRange", required=True,
                        help="日期分区范围（N-不分区、M-月、Q-季、Y-年）")
    parser.add_argument("-orgPos", required=True,
                        help="机构字段位置（1-没有机构字段 "
                             "2-字段在列中 3-字段在分区中）")
    parser.add_argument("-clusterCol", required=True, help="分桶键")
    parser.add_argument("-bucketsNum", required=True, help="分桶数")
    parser.add_argument("-allTableName", required=False,
                        help="全量历史表名（当归档方式为历史增量，"
                             "且源数据为全量时传入），格式 dbname.tablename")
    parser.add_argument("-addTableName", required=False,
                        help="增量历史表名（当归档方式为历史全量，"
                             "且源数据为增量时传入），格式 dbname.tablename")

    args = parser.parse_args()
    return  args

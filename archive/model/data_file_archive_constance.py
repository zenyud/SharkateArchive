# -# - coding: UTF-8 -# -  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from enum import Enum


class DataArchiveConstance(Enum):
    # 归档数据对象
    OBJ_NAME = "obj"

    # 归档机构
    ORG = "org"
    # 源数据存储模式
    SOURCE_DATA_MODE = "sourceDataMode"
    # 源系统名称
    SOURCE_DB_NAME = "sourceDbName"

    # 源表名称
    SOURCE_TBALE_NAME  = "sourceTableName"

    FILTER_SQL = "filterSql"  # 采集过滤SQL条件（WHERE 后面部分）
    FILTER_COL = "filterCol"  # 过滤字段

    SCHEMA_ID = "schemaID" # 获取用户名，密码，以及数据库名

    # 归档目标库
    DB_NAME = "dbName"

    # 归档表
    TABLE_NAME = "tableName"

    SAVE_MODE = "savaMode"  # 归档方式（1-历史全量、2-历史增量、
    # 4-拉链、5-最近一天增量、6-最近一天全量）

    DATA_DATE = "dataDate" # 数据日期

    DATE_RANGE = "dateRange" # 日期范围

    ORG_POS = "orgPos"  # 机构字段位置（1-没有机构字段 2-字段在列中 3-字段在分区中）

    CLUSTER_COL = "clusterCol"  # 分桶键
    BUCKETSNUM = "bucketsNum"   # 分桶数
    ALL_TABLE_NAME = "allTableName" # 全量历史表名（当归档方式为历史增量，
    # 且源数据为全量时传入），格式 dbname.tablename
    ADD_TABLE_NAME = "addTableName" # 增量历史表名（当归档方式为历史全量，
    # 且源数据为增量时传入），格式 dbname.tablename
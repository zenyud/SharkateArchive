# -# - coding: UTF-8 -# -  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from enum import Enum


class DataFileArchiveConstance(Enum):
    # 归档步骤
    ARCHIVE_STEP = "STEP"

    # 归档方式
    ARCHIVE_MODE = "ARCHIVE_MODE"

    # 归档系统
    SYS_NAME = "SYS_NAME"

    # 归档数据对象
    OBJ_NAME = "OBJ_NAME"

    # 归档数据对象ID
    OBJ_ID = "OBJ_ID"

    # 数据对象细分类
    DATA_TYPE = "DATA_TYPE"

    # 归档目标库
    HIVE_DB = "HIVE_DB"

    # 归档表
    TABLE_NAME = "TABLE_NAME"

    # 归档机构
    ORG = "ORG"

    # 归档数据日期
    DATA_DATE = "DATA_DATE"

    # 归档数据开始日期（流水类数据用）
    START_DATA_DATE = "START_DATA_DATE"

    # 数据文件格式类型
    DATA_FORMAT_TYPE = "DATA_FORMAT_TYPE"

    # 归档数据编码格式
    CHARSET = "CHARSET"

    # 归档源数据日期字段格式
    COL_DATE_FORMAT = "COL_DATE_FORMAT"

    # 归档源数据
    INPUT_FILE = "INPUT_FILE"

    # 表定义文件
    DDL_FILE = "DDL_FILE"

    # 控制文件
    CTRL_FILE = "CTRL_FILE"

    # 是否有减量文件
    EXISTS_DEL = "EXISTS_DEL"

    # 集群名
    CLUSTER = "CLUSTER"

    # 归档生命周期
    LIFE_CYCLE = "LIFE_CYCLE"

    # 数据提供模式
    DATA_MODE_SOURCE = "DATA_MODE_SOURCE"

    # 数据模式
    DATA_MODE = "DATA_MODE"

    # MR作业队列
    MR_JOB_QUEUE_NAME = "MR_JOB_QUEUE_NAME"

    # MR作业优先级
    MR_JOB_PRIOTIRY = "MR_JOB_PRIOTIRY"

    # 操作用户
    LOGIN_USER = "LOGIN_USER"

    # 归档策略ID
    STRATEGY_ID = "STRATEGY_ID"

    # 修数步骤
    REVISE_STEP = "STEP"

    # 归档源数据列分隔符
    COLUMN_SEPARATOR = "COLUMN_SEPARATOR"

    # 默认归档表日期区间宽度
    DEFAULT_INCREMENT_SCOPE = "M"
    # 字符集
    FILE_CHARSET = "utf-8"

    FILE_CHARSET_ = "utf8"

    PATH_DATA = "data"

    PATH_CTRL = "ctrl"

    PATH_DDL = "ddl"

    DATA_TAIL = ".dat"

    FILE_DATA = "/data/"

    FILE_CTRL = "/ctrl/"

    FILE_DDL = "/ddl/"

    CTRL_TAIL = ".chk"

    DDL_TAIL = ".ddl"

    STORAGE_SERVICE = "STORAGE_SERVICE"

    # 是否事务表
    TRANSTABLE = "TRANSTABLE"

    # 分桶键
    CLUSTERCOL = "CLUSTERCOL"

    # 分桶数
    BUCKETSNUM = "BUCKETSNUM"

    # 日期分区范围
    DATE_PATITON_RANGE = "DATE_PATITON_RANGE"

    # 机构字段位置
    ORG_POS = "ORG_POS"

    # 开链分区标识日期
    CHAIN_OPENDATE = "99991231"

    # 行尾是否分隔符结尾
    LINE_END_SEPARATOR = "LINE_END_SEPARATOR"

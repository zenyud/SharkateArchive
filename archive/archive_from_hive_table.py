# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 归档入口
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
import re

from archive.date_check import DateCheck
from archive.init_archive import init
from archive.archive_lock import ArchiveLock, MetaLock
from archive.org_check import OrgCheck
from utils.logger import Logger

LOG = Logger()


class ArchiveData(object):
    """数据归档类
        args:参数
    """

    def __init__(self, args):
        self.__args = args

        self.__print_arguments()

    def __print_arguments(self):
        """ 参数格式化输出

        Args:

        Returns:

        Raise:

        """

        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("数据对象名       : {0}".format(self.__args.obj))
        LOG.debug("SCHEMA ID       : {0}".format(self.__args.schemaID))
        LOG.debug("机构号           : {0}".format(self.__args.org))
        LOG.debug("源数据增全量      : {0}".format(self.__args.sourceDataMode))
        LOG.debug("源库名           : {0}".format(self.__args.sourceDbName))
        LOG.debug("源表名           : {0}".format(self.__args.sourceTableName))
        LOG.debug("过滤条件         : {0}".format(self.__args.filterSql))
        LOG.debug("过滤字段         : {0}".format(self.__args.filterCol))
        LOG.debug("归档库名          : {0}".format(self.__args.dbName))
        LOG.debug("归档表名          : {0}".format(self.__args.tableName))
        LOG.debug("归档方式           : {0}".format(self.__args.saveMode))
        LOG.debug("数据日期           : {0}".format(self.__args.dataDate))
        LOG.debug("日期分区范围        : {0}".format(self.__args.dateRange))
        LOG.debug("机构字段位置        : {0}".format(self.__args.orgPos))
        LOG.debug("分桶键             : {0}".format(self.__args.clusterCol))
        LOG.debug("分桶数             : {0}".format(self.__args.bucketsNum))
        LOG.debug("全量历史表名        : {0}".format(self.__args.allTableName))
        LOG.debug("增量历史表名        : {0}".format(self.__args.addTableName))
        LOG.debug("----------------------------------------------")

    def run(self):
        """
        归档程序运行入口
        :return:
         1 - 成功 0 - 失败
        """
        # 任务加锁判断
        LOG.info("判断是否有在进行的任务,并加锁 ")
        ArchiveLock(self.__args.obj, self.__args.org).check()

        LOG.info("日期分区字段检查 ")
        date_check = DateCheck(self.__args.dataDate, self.__args.dateRange,
                               self.__args.dbName, self.__args.tableName)
        date_check.date_partition_check()

        # 机构字段检查
        LOG.info("机构字段字段检查 ")
        org_check = OrgCheck(self.__args.dbName, self.__args.tableName,
                             self.__args.orgPos)
        org_check.check()

        LOG.info("元数据处理、表并发处理")
        meta_lock = MetaLock(self.__args.obj,self.__args.org)
        meta_lock.metalock()

        LOG.info("元数据登记与更新")
        LOG.info("元数据解锁")
        LOG.info("源表与Hive表比较")
        LOG.info("创建Hive表或修改Hive表结构")
        LOG.info("源数据的数据量统计")
        LOG.info("数据载入")
        LOG.info("统计入库条数")
        LOG.info("登记数据资产")
        LOG.info("解除并发锁")

if __name__ == '__main__':
    # 初始化判断
    args = init()
    archive = ArchiveData(args)
    archive.run()

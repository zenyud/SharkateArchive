# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc :  归档任务加锁 解锁
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
import time


from archive.dao.struct_lock_dao import ArchiveLockDao, MetaLockDao
from utils.logger import Logger
from utils.biz_excpetion import BizException

LOG = Logger()
archive_lock_dao = ArchiveLockDao()
meta_lock_dao = MetaLockDao()


class ArchiveLock(object):
    def __init__(self, obj, org):
        self.__obj = obj
        self.__org = org

        # self.__storage_mode = storage_mode

    def find_archive(self):
        """
            判断是否有正在运行的归档任务
        :return: 返回查询条数
        """
        try:
            result = archive_lock_dao.find_by_pk(self.__obj, self.__org)
            return len(result)
        except Exception as e :
            print e.message

    def archive_lock(self):
        """
            对归档任务进行加锁
        :return:
        """
        try:
           archive_lock_dao.add(self.__obj,self.__org)
        except Exception as e :
            raise BizException("待归档表有另外正在归档的任务或后台数据库更新错，请稍后再试。[{0}]".format(e.message))

    def check(self):
        """ 加锁执行
        :return:
        """
        if self.find_archive() == 0:
            self.archive_lock()
        else:
            raise BizException("待归档表有另外正在归档的任务，请稍后再试。")


class MetaLock(object):

    def __init__(self,obj ,org):
        self.__obj = obj
        self.__org = org

    def metalock(self):
        start_time = time.time()
        flag = False
        self.meta_lock_do()
        while not flag:
            if time.time()-start_time>60000:
                raise BizException("元数据更新等待超时,请稍后再试！")
            try:
                time.sleep(1)
            except Exception as e :
                LOG.debug(e)
            self.meta_lock_do()

    def meta_lock_do(self):
        if len(meta_lock_dao.find_by_pk(self.__obj, self.__org)) == 0:
            try:
                meta_lock_dao.add(self.__obj, self.__org)
                flag = True
            except Exception as e:
                LOG.debug("元数据更新队列等待中 。。。 ")


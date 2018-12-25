# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc :  归档任务加锁 解锁
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from archive.model.base_model import *
from utils.base_connect import get_session
from utils.logger import Logger
from utils.biz_excpetion import BizException
SESSION = get_session()
LOG = Logger()


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
            result = SESSION.query(DidpHdsStructArchiveCtrl).\
                filter(DidpHdsStructArchiveCtrl.OBJECT_NAME == self.__obj,
                       DidpHdsStructArchiveCtrl.ORG_CODE == self.__org,
                       ).all()
            return len(result)
        except Exception as e :
            print e.message

    def archive_lock(self):
        """
            对归档任务进行加锁
        :return:
        """
        try:
            obj = DidpHdsStructArchiveCtrl(OBJECT_NAME=self.__obj,ORG_CODE=self.__org)
            SESSION.add(obj)
            SESSION.commit()
        except Exception as e :
            raise BizException("待归档表有另外正在归档的任务或后台数据库更新错，请稍后再试。[{0}]".format(e.message))

    def check(self):
        """ 加锁执行
        :return:
        """
        if self.find_archive() ==0 :
            self.archive_lock()
        else:
            raise BizException("待归档表有另外正在归档的任务，请稍后再试。")

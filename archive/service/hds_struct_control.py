# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/29
# Write By      : adtec(ZENGYU)
# Function Desc :  并发控制模块
# History       : 2018/12/29  ZENGYU     Create
# Remarks       :
from archive.dao.struct_lock_dao import ArchiveLockDao, MetaLockDao


class HdsStructControl(object):
    archive_lock_dao = ArchiveLockDao()
    meta_lock_dao = MetaLockDao()

    def find_archive(self, obj, org):
        """
            查看是否有正在执行的归档任务
        :param obj: 归档对象
        :param org: 归档机构
        :return:
        """
        try:
            result = HdsStructControl.archive_lock_dao.find_by_pk(obj, org)
            if len(result) == 0:
                return None
            else:
                return result
        except Exception as e:
            print e.message

    def archive_lock(self, obj, org):
        """
            对归档任务进行加锁
        :return:
        """
        HdsStructControl.archive_lock_dao.add(obj, org)


    @staticmethod
    def archive_unlock(obj, org):
        """
            归档任务解锁
        :param obj:
        :param org:
        :return:
        """
        HdsStructControl.archive_lock_dao.delete_by_pk(obj, org)

    def meta_lock_find(self, obj, org):
        """
            元数据锁查找
        :return:
        """
        r = self.meta_lock_dao.find_by_pk(obj, org)
        if len(r) == 0:
            return None
        else:
            return r

    def meta_lock(self, obj, org):
        """
            元数据加锁
        :return:
        """
        self.meta_lock_dao.add(obj, org)
        pass

    def meta_unlock(self, obj, org):
        """
                   解除元数据控制锁
               :return:
               """

        self.meta_lock_dao.delete_by_pk(obj, org)

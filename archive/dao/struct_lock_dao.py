# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/25
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/25  ZENGYU     Create
# Remarks       :
from archive.model.base_model import DidpHdsStructArchiveCtrl, \
    DidpHdsStructMetaCtrl, HdsStructArchiveCtrl
from utils.base_connect import get_session
from utils.logger import Logger

SESSION = get_session()
LOG = Logger()


class ArchiveLockDao(object):
    """
        归档控制Dao
    """

    def add(self, obj, org):
        didp_archive_ctrl = DidpHdsStructArchiveCtrl(OBJECT_NAME=obj,
                                                     ORG_CODE=org)
        SESSION.add(didp_archive_ctrl)
        SESSION.commit()

    def delete_by_pk(self, obj, org):
        SESSION.query(DidpHdsStructArchiveCtrl). \
            filter(DidpHdsStructArchiveCtrl.OBJECT_NAME == obj,
                   DidpHdsStructArchiveCtrl.ORG_CODE == org).delete()
        SESSION.commit()

    def find_by_pk(self, obj, org):
        """
            通过主键查找
        :param obj: 数据对象名
        :param org: 机构名
        :return:  查询结果
        """
        result = SESSION.query(DidpHdsStructArchiveCtrl). \
            filter(DidpHdsStructArchiveCtrl.OBJECT_NAME == obj,
                   DidpHdsStructArchiveCtrl.ORG_CODE == org,
                   ).all()
        return result


class MetaLockDao(object):
    """
        元数据控制Dao
    """

    def add(self, obj, org):
        mate_ctrl = DidpHdsStructMetaCtrl(OBJECT_NAME=obj,
                                          ORG_CODE=org)
        SESSION.add(mate_ctrl)
        SESSION.commit()

    def delete_by_pk(self, obj, org):
        SESSION.query(DidpHdsStructMetaCtrl). \
            filter(DidpHdsStructMetaCtrl.OBJECT_NAME == obj,
                   DidpHdsStructMetaCtrl.ORG_CODE == org).delete()
        SESSION.commit()

    def find_by_pk(self, obj, org):
        """
            通过主键查找
        :param obj: 数据对象名
        :param org: 机构名
        :return:  查询结果
        """
        result = SESSION.query(DidpHdsStructMetaCtrl). \
            filter(DidpHdsStructMetaCtrl.OBJECT_NAME == obj,
                   DidpHdsStructMetaCtrl.ORG_CODE == org,
                   ).all()
        return result


if __name__ == '__main__':

    print SESSION.query(DidpHdsStructArchiveCtrl).all()

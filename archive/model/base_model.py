# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/24
# Write By      : adtec(ZENGYU)
# Function Desc :  基础类模块
# History       : 2018/12/24  ZENGYU     Create
# Remarks       :
from utils.base_connect import get_base
from sqlalchemy import *
Base = get_base()


class HdsStructArchiveCtrl(Base):
    """
        映射HDS_STRUCT_ARCHIVE_CTRLS 表
    """
    __tablename__ = 'HDS_STRUCT_ARCHIVE_CTRL'
    # SYSTEM_NAME = Column(primary_key=True)
    OBJECT_NAME = Column(primary_key=True)
    STATUS = Column()
    # CREATE_USER = Column()
    # CREATE_TIME = Column()
    ORG_CODE = Column(primary_key=True)
    STORAGE_MODE = Column(primary_key=True)


class DidpHdsStructArchiveCtrl(Base):
    """
        映射HDS_STRUCT_ARCHIVE_CTRLS 表
    """
    __tablename__ = 'DIDP_HDS_STRUCT_ARCHIVE_CTRL'
    # SYSTEM_NAME = Column(primary_key=True)
    OBJECT_NAME = Column(primary_key=True)
    # STATUS = Column()
    # CREATE_USER = Column()
    # CREATE_TIME = Column()
    ORG_CODE = Column(primary_key=True)
    # STORAGE_MODE = Column(primary_key=True)

class DidpCommonParams(Base):
    __tablename__ = "DIDP_COMMON_PARAMS"
    PARAM_ID = Column(primary_key=True,nullable=False)
    LAST_UPDATE_TIME = Column(nullable=False)
    LAST_UPDATE_USER = Column(nullable=False)
    GROUP_NAME = Column(nullable=False)
    PARAM_NAME = Column(nullable=True)
    PARAM_VALUE = Column(nullable=True)
    DESCRIPTION = Column(nullable=True)
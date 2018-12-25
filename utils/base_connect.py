# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/24
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/24  ZENGYU     Create
# Remarks       :
import ConfigParser
import os

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import *
import ibm_db_sa


def get_base():
    Base = declarative_base()
    return Base


def get_session():
    engine = create_engine("ibm_db_sa://{db_user}"
                           ":{password}@{db_url}:{port}/dbName"
                           .format(db_user=user, password=password,
                                   db_url=db_url, port=port,
                                   dbName=dbName))
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def get_root_path():
    """
    获取配置文件的绝对路径
    :return:
    """
    # 获取当前路径
    curPath = os.path.abspath(os.path.dirname(__file__))
    rootPath = curPath[:curPath.find("SharkateArchive\\") + len(
        "SharkateArchive\\")]  # SharkateArchive，也就是项目的根路径

    return rootPath


cf = ConfigParser.ConfigParser()
cf.read(get_root_path() + "conf\\db.ini")
user = cf.get("db2", "user")
password = cf.get("db2", "password")
db_url = cf.get("db2", "db_url")
port = cf.get("db2", "port")
dbName = cf.get("db2", "dbName")

if __name__ == '__main__':
    print

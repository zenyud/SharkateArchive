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

from utils.get_conf import get_root_path
from utils.logger import Logger
from sqlalchemy.ext.declarative import declarative_base
LOG = Logger()

cf = ConfigParser.ConfigParser()
cf.read(get_root_path() + "conf\\db.ini")
user = cf.get("db2", "user")
password = cf.get("db2", "password")
db_url = cf.get("db2", "db_url")
port = cf.get("db2", "port")
dbName = cf.get("db2", "dbName")

Base = declarative_base()
LOG.debug("——————")
LOG.debug("-----数据库连接信息----")
LOG.debug("数据库用户    ：{0}".format(user))
LOG.debug("密码         ：{0}".format(password))
LOG.debug("数据库类型       ：{0}".format("db2"))
LOG.debug("JDBC URL地址   ：{0}".format(db_url))

def get_session():
    engine_str = "ibm_db_sa://{db_user}:{password}@{db_url}:" \
                 "{port}/{dbName}".format(
        db_user=user, password=password,
        db_url=db_url, port=port,
        dbName=dbName)


    engine = create_engine(engine_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

if __name__ == '__main__':
    pass







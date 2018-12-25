# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/24
# Write By      : adtec(ZENGYU)
# Function Desc : Hive 工具类
# History       : 2018/12/24  ZENGYU     Create
# Remarks       :
import ConfigParser

from archive.archive_enum.archive_enum import PartitionKey, AddColumn
from archive.model.base_model import DidpCommonParams
from utils.base_connect import get_session, get_root_path
from utils.db_operator import DbOperator

SESSION = get_session()
root_path = get_root_path()
cf = ConfigParser.ConfigParser()
cf.read(root_path + 'conf\\db.ini')
user = cf.get("inceptor", "user")
password = cf.get("inceptor", "password")
db_url = cf.get("inceptor", "db_url")
dbName = cf.get("inceptor", "dbName")
driverClass = cf.get("inceptor", "driverClass")
driverName = cf.get("inceptor", "driverName")
driverPath = root_path+"drivers\\{0}".format(driverName)
db_oper = DbOperator(user,password,driverClass,db_url,driverPath)


class HiveUtil(object):
    @staticmethod
    def exist_table(db_name, table_name):
        """
        :param db_name: 数据库名
        :param table_name:  数据表名
        :param conn: 连接对象
        :return:
        """
        sql1 = "use {dbName}".format(dbName=db_name)
        sql = "show tables '{tableName}'".format(tableName=table_name)
        db_oper.connect()
        db_oper.do(sql1)
        result = db_oper.fetchall(sql)
        db_oper.close()

        return len(result) == 1
    @staticmethod
    def has_partition(db_name, table_name):
        """
        :param db_name: 数据库名
        :param table_name:  表名
        :param db_oper: 数据库操作对象
        :return: 是否存在分区
        """
        sql1 = "use {dbName}".format(dbName=db_name)
        sql = "desc formatted  {tableName}".format(tableName=table_name)
        db_oper.connect()
        db_oper.do(sql1)
        result = db_oper.fetchall(sql)
        db_oper.close()
        # 在数据库中获取时间分区键的名称
        didp_params = SESSION.query(DidpCommonParams).filter(
            DidpCommonParams.GROUP_NAME
            == PartitionKey.GROUP,
            DidpCommonParams.PARAM_NAME
            == PartitionKey.DATE_SCOPE).all()

        pri_key = didp_params.PARAM_VALUE
        # print  pri_key
        flag = False
        for col in result:
            if str(col[0]).strip().upper().__eq__(pri_key.upper()):
                flag = True
                break
        return flag
    @staticmethod
    def get_org_pos(db_name, table_name):
        """
            获取机构分区字段
        :param db_name:
        :param table_name:
        :return:
        """
        sql1 = "use {database}".format(database=db_name)
        sql = "desc formatted {table}".format(table=table_name)
        db_oper.connect()
        db_oper.do(sql1)
        result = db_oper.fetchall(sql)
        db_oper.close()
        # 在数据库中获取机构分区键的位置
        p_key = SESSION.query(DidpCommonParams).filter(
            DidpCommonParams.GROUP_NAME
            == PartitionKey.GROUP,
            DidpCommonParams.PARAM_NAME
            == PartitionKey.ORG).all().PARAM_VALUE
        a_key = SESSION.query(DidpCommonParams).filter(
            DidpCommonParams.GROUP_NAME
            == AddColumn.GROUP,
            DidpCommonParams.PARAM_NAME
            == AddColumn.COL_ORG).all().PARAM_VALUE
        key = ""
        for col in result:
            if col[0].strip().upper().__eq__(p_key.upper()):
                key = p_key
                break

            elif col[0].strip().upper().__eq__(a_key.upper()):
                key = a_key
                break

        return key

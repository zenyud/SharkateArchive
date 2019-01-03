# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/24
# Write By      : adtec(ZENGYU)
# Function Desc : Hive 工具类
# History       : 2018/12/24  ZENGYU     Create
# Remarks       :
import ConfigParser

from archive.archive_enum.archive_enum import PartitionKey, AddColumn
from archive.dao.common_param_dao import CommonParamsDao
from archive.model.base_model import DidpCommonParams
from archive.model.hive_field_info import HiveFieldInfo
from utils.base_connect import get_session, get_root_path
from utils.db_operator import DbOperator
from utils.str_utils import StringUtil

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
        result = HiveUtil.get_table_desc(db_name, table_name)
        # 在数据库中获取时间分区键的名称
        didp_params = SESSION.query(DidpCommonParams).filter(
            DidpCommonParams.GROUP_NAME
            == PartitionKey.GROUP.value,
            DidpCommonParams.PARAM_NAME
            == PartitionKey.DATE_SCOPE.value).all()

        pri_key = didp_params[0].PARAM_VALUE

        flag = False
        for col in result:
            print col[0]
            if StringUtil.eq_ignore(col[0],pri_key):

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
        result = HiveUtil.get_table_desc(db_name,table_name)
        # 在数据库中获取机构分区键的位置
        p_key = SESSION.query(DidpCommonParams).filter(
            DidpCommonParams.GROUP_NAME
            == PartitionKey.GROUP.value,
            DidpCommonParams.PARAM_NAME
            == PartitionKey.ORG.value).one().PARAM_VALUE
        a_key = SESSION.query(DidpCommonParams).filter(
            DidpCommonParams.GROUP_NAME
            == AddColumn.GROUP.value,
            DidpCommonParams.PARAM_NAME
            == AddColumn.COL_ORG.value).one().PARAM_VALUE
        # 机构字段位置（1-没有机构字段 2-字段在列中 3-字段在分区中）
        key = 1
        for col in result:
            if col[0].strip().upper().__eq__(p_key.upper()):
                key = 3
                break

            elif col[0].strip().upper().__eq__(a_key.upper()):
                key = 2
                break

        return key

    @staticmethod
    def get_table_descformatted(db_name, table_name):
        sql1 = "use {database}".format(database=db_name)
        sql = "desc formatted {table}".format(table=table_name)
        db_oper.connect()
        db_oper.do(sql1)
        result = db_oper.fetchall(sql)
        db_oper.close()
        return result

    @staticmethod
    def get_table_desc(db_name,table_name):
        sql1 = "use {database}".format(database=db_name)
        sql = "desc {table}".format(table=table_name)
        db_oper.connect()
        db_oper.do(sql1)
        result = db_oper.fetchall(sql)
        db_oper.close()
        return result

    @staticmethod
    def execute(sql):
        db_oper.execute(sql)
    @staticmethod
    def execute_sql(sql):
        """
        有返回结果
        :param self:
        :return:
        """
        return db_oper.fetchall_direct(sql)
    @staticmethod
    def get_common_dict():
        return CommonParamsDao().get_all_common_code()

    @staticmethod
    def get_hive_meta_field(db_name, table_name, filter):
        # type: (str, str, bool) -> list(HiveFieldInfo)
        """
            获取Hive的元数据信息
        :param db_name:
        :param table_name:
        :param filter: 是否过滤添加字段
        :return:  字段信息列表
        """
        result = HiveUtil.get_table_desc(db_name,table_name)
        add_cols = set()
        partition_cols = set()
        if filter :
            common_dict = HiveUtil.get_common_dict()
            print "common_dict len :" + str(common_dict.__len__())
            for add_col in AddColumn:
                print add_col.value
                add_cols.add(common_dict.get(add_col.value).upper().strip())
            for part_col in PartitionKey:
                partition_cols.add(common_dict.get(part_col.value).upper().strip)
        i = 0
        hive_meta_info_list = list() # 字段信息列表
        # 迭代字段
        for x in result:
            if add_cols.__contains__(x[0].upper().strip()):
                continue
            if partition_cols.__contains__(x[0].upper().strip()):
                continue
            if x[0].__contains__("#") or StringUtil.is_blank(x[0]):
                continue
            hive_mate_info = HiveFieldInfo(x[0].upper(),
                                           x[1],x[2],x[3],x[4],x[5].strip(),i)

            hive_meta_info_list.append(hive_mate_info)
            i = i+1

        return hive_meta_info_list

    @staticmethod
    def get_table_comment(db_name, table_name):
        desc_formmatted = HiveUtil.get_table_descformatted(db_name,
                                                           table_name)
        result = ""
        for attr in desc_formmatted:
            if attr[0].strip().upper().__eq__("COMMENT"):
                result = attr[1].strip()

        return result

if __name__ == '__main__':
    a = HiveUtil.has_partition("default","test_archive")
    print a
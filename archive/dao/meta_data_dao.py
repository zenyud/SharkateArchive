# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/25
# Write By      : adtec(ZENGYU)
# Function Desc : 元数据访问模块
# History       : 2018/12/25  ZENGYU     Create
# Remarks       :
from utils.base_connect import get_session
from archive.model.base_model import *
from utils.str_utils import StringUtil

SESSION = get_session()


class MetaColumnInfoDao(object):
    """
        元数据字段信息访问
    """

    @staticmethod
    def delete_all_column(table_id):
        SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID==table_id).delete()
        SESSION.commit()
        SESSION.close()

    @staticmethod
    def get_meta_data_by_table(table_id):
        result = SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID == table_id).all()
        SESSION.close()
        return result

    @staticmethod
    def add_meta_column(meta_field_info):
        """
            添加字段元数据
        :param meta_field_info: 字段元数据对象
        :return:
        """
        SESSION.add(meta_field_info)
        SESSION.commit()
        SESSION.close()

    @staticmethod
    def update_meta_column(table_id, col_name, update_dict):
        """
            更新字段
        :param table_id: 表id
        :param col_name: 字段名
        :param update_dict: 更新字典
        :return:
        """
        SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID == table_id,
            DidpMetaColumnInfo.COL_NAME == col_name).update(update_dict)

        SESSION.commit()
        SESSION.close()


class MetaColumnInfoHisDao(object):
    @staticmethod
    def update_meta_column_his(table_id, column_name, update_dict):
        """
            更新表元数据字段
        :param table_id: 表ID
        :param column_name: 字段名
        :param update_dict: 更新内容
        :return:
        """
        SESSION.query(DidpMetaColumnInfoHis).filter(
            DidpMetaColumnInfoHis.TABLE_ID == table_id,
            DidpMetaColumnInfoHis.COL_NAME == column_name).update(update_dict)
        SESSION.commit()
        SESSION.close()

    @staticmethod
    def add_meta_column_his(meta_field_info_his):
        """
            添加字段元数据
        :param meta_field_info_his: 字段元数据对象
        :return:
        """
        SESSION.add(meta_field_info_his)
        SESSION.commit()
        SESSION.close()

    @staticmethod
    def get_meta_column_info(table_his_id):
        """
                获取字段的信息
        :param table_his_id:
        :return:
        """
        result = SESSION.query(DidpMetaColumnInfoHis).filter(
            DidpMetaColumnInfoHis.TABLE_HIS_ID == table_his_id).all()
        SESSION.close()
        return result


class MetaTableInfoDao(object):
    """
     表元数据信息表
    """

    def get_recent_meta_table_info(self, table_name, release_date):
        """
            获取最近的表元数据信息
        :param table_name:
        :param release_date:
        :return: 最近一天的元数据信息
        """
        result = SESSION.query(DidpMetaTableInfo).filter(
            DidpMetaTableInfo.TABLE_NAME == table_name,
            DidpMetaTableInfo.RELEASE_DATE <= release_date).order_by(
            DidpMetaTableInfo.RELEASE_DATE.desc()).all()

        if len(result) == 0:
            result = SESSION.query(DidpMetaTableInfo).filter(
                DidpMetaTableInfo.TABLE_NAME == table_name,
                DidpMetaTableInfo.RELEASE_DATE >= release_date).order_by(
                DidpMetaTableInfo.RELEASE_DATE.asc()).all()
        if len(result) > 0:
            return result[0]
        else:
            return null

    def add_meta_table_info(self, meta_table_info):
        """

        :param meta_table_info: 表元数据对象
        :return:
        """

        SESSION.add(meta_table_info)
        SESSION.commit()

    def get_meta_table_info(self, schema_id, table_name):
        """
            获取Meta_table_info
        :param schema_id: SCHEMA_ID
        :param table_name: 表名
        :return:
        """
        meta_table_info = SESSION.query(DidpMetaTableInfo).filter(
            DidpMetaTableInfo.SCHEMA_ID == schema_id,
            DidpMetaTableInfo.TABLE_NAME == table_name).all()
        SESSION.close()
        if len(meta_table_info) == 0:
            return None
        return meta_table_info

    def get_meta_table_info_by_time(self, table_name, release_time):
        """
            通过表名，日期获取Meta_table_info
        :param table_name:
        :param release_time:
        :return:
        """
        meta_table_info = SESSION.query(DidpMetaTableInfo).filter(
            DidpMetaTableInfo.TABLE_NAME == table_name,
            DidpMetaTableInfo.RELEASE_DATE == release_time).all()
        SESSION.close()
        return meta_table_info

    def delete_meta_table_info(self, schema_id, table_name):
        """
            删除MataTableInfo
        :param schema_id:
        :param table_name:
        :return:
        """

        SESSION.query(DidpMetaTableInfo).filter(
            DidpMetaTableInfo.SCHEMA_ID == schema_id,
            DidpMetaTableInfo.TABLE_NAME == table_name).delete()
        SESSION.commit()
        SESSION.close()

    @staticmethod
    def update_meta_table_info(schema_id, table_name, update_dict):
        """
            更新表元数据信息
        :param schema_id:
        :param table_name:
        :param update_dict:
        :return:
        """
        SESSION.query(DidpMetaTableInfo) \
            .filter_by(TABLE_NAME=table_name,
                       SCHEMA_ID=schema_id).update(update_dict)
        SESSION.commit()
        SESSION.close()


class MetaTableInfoHisDao(object):
    @staticmethod
    def update_meta_table_info_his(table_his_id, update_dict):
        """
                更新历史表元数据信息
        :param table_his_id:
        :param update_dict:
        :return:
        """
        SESSION.query(DidpMetaTableInfoHis).filter(
            DidpMetaTableInfoHis.TABLE_HIS_ID == table_his_id).update(
            update_dict)
        SESSION.commit()
        SESSION.close()

    @staticmethod
    def add_meta_table_info_his(meta_table_info_his):
        SESSION.add(meta_table_info_his)
        SESSION.commit()
        SESSION.close()

    @staticmethod
    def get_meta_table_info_his_list(table_id, schema_id, data_date):
        result = SESSION.query(DidpMetaTableInfoHis).filter(
            DidpMetaTableInfoHis.TABLE_ID == table_id,
            DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
            DidpMetaTableInfoHis.RELEASE_DATE == data_date).all()
        SESSION.close()
        return result

    @staticmethod
    def get_meta_table_info_his(table_his_id):
        result = SESSION.query(DidpMetaTableInfoHis).filter(
            DidpMetaTableInfoHis.TABLE_HIS_ID == table_his_id
        ).one()
        return result

    @staticmethod
    def get_before_meta_table_infos(schema_id, table_name, data_date):
        """
            获取data_date 之前的表元数据版本
        :param schema_id:
        :param table_name: 表名
        :param data_date: 归档日期
        :return: List<DidpMetaTableInfo>
        """
        result = SESSION.query(DidpMetaTableInfoHis). \
            filter(DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
                   DidpMetaTableInfoHis.TABLE_NAME == table_name,
                   DidpMetaTableInfoHis.RELEASE_DATE < data_date). \
            order_by(DidpMetaTableInfoHis.RELEASE_DATE.desc()).all()
        SESSION.close()
        return result

    @staticmethod
    def get_after_meta_table_infos(schema_id, table_name, data_date):
        """
                获取data_date 之后的表元数据版本
            :param schema_id:
            :param table_name: 表名
            :param data_date: 归档日期
            :return: List<DidpMetaTableInfo>
            """
        result = SESSION.query(DidpMetaTableInfoHis). \
            filter(
            DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
            DidpMetaTableInfoHis.TABLE_NAME == table_name,
            DidpMetaTableInfoHis.RELEASE_DATE > data_date). \
            order_by(DidpMetaTableInfoHis.RELEASE_DATE.asc()).all()
        SESSION.close()
        return result

    @staticmethod
    def get_meta_table_info_by_time(schema_id, table_name, data_date):
        """
            可能会返回多个结果
        :param schema_id:
        :param table_name:
        :param data_date:
        :return:
        """
        result = SESSION.query(DidpMetaTableInfoHis). \
            filter(
            DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
            DidpMetaTableInfoHis.TABLE_NAME == table_name,
            DidpMetaTableInfoHis.RELEASE_DATE == data_date).all()
        SESSION.close()
        return result

    @staticmethod
    def get_meta_table_info_by_detail(schema_id, table_name, data_date,
                                      bucket_num, comment,
                                      table_comment_change_ddl):
        """
            根据详细的字段信息来获取表的元数据
        :param schema_id:
        :param table_name:
        :param data_date:
        :param bucket_num:
        :param comment:
        :param table_comment_change_ddl : 表备注改变是否新增表版本 "true" "false"
        :return:
        """
        if StringUtil.eq_ignore(table_comment_change_ddl, "true"):
            result = SESSION.query(DidpMetaTableInfoHis). \
                filter(
                DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
                DidpMetaTableInfoHis.TABLE_NAME == table_name,
                DidpMetaTableInfoHis.RELEASE_DATE == data_date,
                DidpMetaTableInfoHis.BUCKET_NUM == bucket_num,
                DidpMetaTableInfoHis.DESCRIPTION == comment
            ).all()
        else:
            result = SESSION.query(DidpMetaTableInfoHis). \
                filter(
                DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
                DidpMetaTableInfoHis.TABLE_NAME == table_name,
                DidpMetaTableInfoHis.RELEASE_DATE == data_date,
                DidpMetaTableInfoHis.BUCKET_NUM == bucket_num
            ).all()
        SESSION.close()
        return result

        pass


if __name__ == '__main__':
    a = MetaColumnInfoHisDao.get_meta_column_info(
        "c09d72c00f3811e9b135d0577b506f4e")
    for x in a:
        print x

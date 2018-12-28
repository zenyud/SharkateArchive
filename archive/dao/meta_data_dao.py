# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/25
# Write By      : adtec(ZENGYU)
# Function Desc : 元数据访问模块
# History       : 2018/12/25  ZENGYU     Create
# Remarks       :
from utils.base_connect import get_session
from archive.model.base_model import *

SESSION = get_session()


class MetaColumnInfoDao(object):
    """
        元数据字段信息访问
    """

    def get_meta_data_by_table(self, table_id):
        result = SESSION.query(DidpMetaColumnInfo).filter(
            DidpMetaColumnInfo.TABLE_ID == table_id).all()

        return result

    def add_meta_column(self, meta_field_info):
        """
            添加字段元数据
        :param meta_field_info: 字段元数据对象
        :return:
        """
        SESSION.add(meta_field_info)
        SESSION.commit()
        SESSION.close()

    def update_meta_column(self, table_id, col_name, update_dict):
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
    def add_meta_column_his(self, meta_field_info_his):
        """
            添加字段元数据
        :param meta_field_info: 字段元数据对象
        :return:
        """
        SESSION.add(meta_field_info_his)
        SESSION.commit()
        SESSION.close()


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
        if len(result) > 0 :
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

    @staticmethod
    def get_before_meta_table_infos(table_name, data_date):
        """
            获取data_date 之前的表元数据版本
        :param table_name: 表名
        :param data_date: 归档日期
        :return: List<DidpMetaTableInfo>
        """
        result = SESSION.query(DidpMetaTableInfo). \
            filter(DidpMetaTableInfo.TABLE_NAME == table_name,
                   DidpMetaTableInfo.RELEASE_DATE < data_date). \
            order_by(DidpMetaTableInfo.RELEASE_DATE.desc()).all()
        SESSION.close()
        return result

    @staticmethod
    def get_after_meta_table_infos(table_name, data_date):
        """
                获取data_date 之后的表元数据版本
            :param table_name: 表名
            :param data_date: 归档日期
            :return: List<DidpMetaTableInfo>
            """
        result = SESSION.query(DidpMetaTableInfo). \
            filter(DidpMetaTableInfo.TABLE_NAME == table_name,
                   DidpMetaTableInfo.RELEASE_DATE > data_date). \
            order_by(DidpMetaTableInfo.RELEASE_DATE.asc()).all()
        SESSION.close()
        return result

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

    def update_meta_table_info(self, table_name, release_date, update_dict):
        """
            更新Meta_table_info
        :param table_name: 表名
        :param release_date: 发布日期
        :param update_dict: 更新字典 {"user_id":1}
        :return:
        """
        meta_table_info = SESSION.query(DidpMetaTableInfo) \
            .filter_by(TABLE_NAME=table_name,
                       RELEASE_DATE=release_date).update(update_dict)
        SESSION.commit()
        SESSION.close()


class MetaTableInfoHisDao(object):
    def add_meta_table_info_his(self, meta_table_info_his):
        SESSION.add(meta_table_info_his)
        SESSION.commit()
        SESSION.close()

    def get_meta_table_info_his(self, table_id, schema_id, data_date):
        result = SESSION.query(DidpMetaTableInfoHis).filter(
            DidpMetaTableInfoHis.TABLE_ID == table_id,
            DidpMetaTableInfoHis.SCHEMA_ID == schema_id,
            DidpMetaTableInfoHis.RELEASE_DATE == data_date).one()
        return result


if __name__ == '__main__':
    a = MetaTableInfoDao().get_meta_table_info("asd", "adw")
    print a

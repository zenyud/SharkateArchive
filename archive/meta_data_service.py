# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/25
# Write By      : adtec(ZENGYU)
# Function Desc :  获取元数据信息
# History       : 2018/12/25  ZENGYU     Create
# Remarks       :

from archive.dao.meta_data_dao import *
from archive.model.base_model import *
from archive.model.source_field_info import SourceFieldInfo
from utils.base_util import get_uuid
from utils.biz_excpetion import BizException
from utils.date_util import DateUtil
from utils.get_conf import get_conf_reader
from utils.hive_utils import HiveUtil
from utils.logger import Logger

meta_table_info_dao = MetaTableInfoDao()
meta_table_info_his_dao = MetaTableInfoHisDao()
meta_column_info_dao = MetaColumnInfoDao()
meta_column_info_his_dao = MetaTableInfoHisDao()
LOG = Logger()


class MetaDataService(object):
    def __init__(self, schema_id, source_db_name, source_table_name, db_name,
                 table_name,
                 data_date, bucket_num):
        self.schema_id = schema_id
        self.source_db_name = source_db_name
        self.source_table_name = source_table_name
        self.db_name = db_name
        self.table_name = table_name
        self.data_date = data_date
        self.bucket_num = bucket_num

    def parse_input_table(self):
        """
            解析接入表结构
        :return:
        """
        source_field_infos = []  # 获取接入数据字段列表
        cols = HiveUtil.get_table_desc(self.source_db_name,
                                       self.source_table_name)

        for col in cols:
            filed_info = SourceFieldInfo(col[0], col[1], col[2], col[3], col[4],
                                         col[5])
            source_field_infos.append(filed_info)

        return source_field_infos

    def upload_meta_data(self):
        """
        登记元数据
        :return:
        """
        # 检查当日是否已经登记元数据
        LOG.info("———————元数据登记检查——————")
        meta_table_info = meta_table_info_dao.get_meta_table_info_by_time(
            self.table_name, self.data_date)

        if len(meta_table_info) != 0:
            # 当日已登记元数据，需要对此元数据进行对比
            if self.check(self.bucket_num, meta_table_info[0]):
                LOG.debug("今日表元数据已登记 ！")
                return
            else:
                # 对表元数据进行更新
                meta_table_info_dao.update_meta_table_info(self.table_name,
                                                           self.data_date,
                                                           self.bucket_num)

        try:
            LOG.info("接入表信息解析")
            source_field_info = self.parse_input_table()
            length = len(source_field_info)
            if length == 0:
                raise BizException("接入表信息解析失败！请检查接入表是否存在 ")
            LOG.info("接入表字段数为：{0}".format(length))
            self.register_meta_data(source_field_info)
        except Exception as e:
            LOG.error("登记元数据失败！ ")
            LOG.error(e.message)

    def register_meta_data(self, source_field_info):
        """
            登记元数据
        :return:
        """
        self.ddl_check(source_field_info)

    def check(self, buckect_num, meta_table_info):
        """
            已登记的元数据与现元数据进行比对
        :return: True 表示一致，False表示不一致
        """
        if meta_table_info.BUCKECT_NUM == buckect_num:
            return True
        else:
            return False

    def ddl_check(self, source_field_info):
        """
            接入表DDL 与 元数据DDL进行比对
        :return:
        """
        # 先查找data_date 日期前后是否存在元数据记录
        before_table_infos = meta_table_info_dao.get_before_meta_table_infos(
            self.table_name, self.data_date)

        after_table_infos = meta_table_info_dao.get_after_meta_table_infos(
            self.table_name, self.data_date)

        if len(before_table_infos) == 0 and len(after_table_infos) == 0:
            # 说明是新的归档数据 直接登记元数据
            table_id = get_uuid()
            conf_reader = get_conf_reader("conf", "base_conf.ini")
            last_update_user = conf_reader.get("update_user", "update_user")
            last_update_time = DateUtil.get_now_date()
            # 登记表元数据
            new_meta_table_info = DidpMetaTableInfo(
                TABLE_ID=table_id,
                SCHEMA_ID=self.schema_id,
                LAST_UPDATE_TIME=last_update_time,
                LAST_UPDATE_USER=last_update_user,
                TABLE_NAME=self.table_name,
                BUCKET_NUM=self.bucket_num,
                RELEASE_DATE=self.data_date)

            table_his_id = get_uuid()  # 表历史id

            new_meta_table_info_his = DidpMetaTableInfoHis(
                TABLE_HIS_ID=table_his_id,
                TABLE_ID=table_id,
                SCHEMA_ID=self.schema_id,
                LAST_UPDATE_TIME=last_update_time,
                LAST_UPDATE_USER=last_update_user,
                TABLE_NAME=self.table_name,
                BUCKET_NUM=self.bucket_num,
                RELEASE_DATE=self.data_date
            )
            # 写入表元数据表
            LOG.info("表元数据登记")
            meta_table_info_dao.add_meta_table_info(new_meta_table_info)
            LOG.info("表元数据登记成功！")
            # 写入表元数据历史表
            LOG.info("表历史元数据登记")
            meta_table_info_his_dao.add_meta_table_info_his(
                new_meta_table_info_his)
            LOG.info("表历史元数据登记成功 ！ ")
            # 登记字段元数据
            LOG.info("登记字段元数据 ")
            for filed in source_field_info:
                meta_field_info = DidpMetaColumnInfo(
                    COLUMN_ID=get_uuid(),
                    TABLE_ID=table_id,
                    LAST_UPDATE_TIME=last_update_time,
                    LAST_UPDATE_USER=last_update_user,
                    COL_SEQ=source_field_info.index(filed),
                    COL_NAME=filed.col_name,
                    COL_DESC=filed.comment,
                    COL_TYPE=filed.data_type,
                    COL_LENGTH=filed.col_length,
                    COL_SCALE=filed.col_scale,
                    COL_DEFAULT=filed.default_value,
                    NULL_FLAG=filed.not_null)

                meta_column_info_dao.add_meta_column(meta_field_info)

                meta_field_info_his = DidpMetaColumnInfoHis(
                    TABLE_HIS_ID=table_his_id,
                    COLUMN_ID=get_uuid(),
                    TABLE_ID=table_id,
                    LAST_UPDATE_TIME=last_update_time,
                    LAST_UPDATE_USER=last_update_user,
                    COL_SEQ=source_field_info.index(filed),
                    COL_NAME=filed.col_name,
                    COL_DESC=filed.comment,
                    COL_TYPE=filed.data_type,
                    COL_LENGTH=filed.col_length,
                    COL_SCALE=filed.col_scale,
                    COL_DEFAULT=filed.default_value,
                    NULL_FLAG=filed.not_null
                )

                meta_column_info_his_dao.add_meta_table_info_his(
                    meta_field_info_his)
            LOG.info("登记字段元数据成功 ！  ")
        else:
            before_column_info = []
            afrer_column_info = []
            # 判断前一条是否为空
            if len(before_table_infos) != 0:
                # 获取字段记录信息
                before_column_info = meta_column_info_dao.get_meta_data_by_table(
                    before_table_infos[0].TABLE_ID)
            if len(after_table_infos) != 0:
                afrer_column_info = meta_column_info_dao.get_meta_data_by_table(
                    after_table_infos[0].TABLE_ID
                )
            LOG.debug("接入数据的字段个数为：{0}".format(len(source_field_info)))
            for s_f in source_field_info:
                LOG.debug(s_f)
            LOG.debug("最早日期数据字段个数为: {0}".format(len(before_column_info)))
            for b_f in before_column_info:
                LOG.debug(b_f)
            LOG.debug("最晚日期数据字段个数为：{0}".format(len(afrer_column_info)))
            for a_f in afrer_column_info:
                LOG.debug(a_f)
            # 调用比较方法
            is_same_as_before = self.get_change_result(source_field_info,
                                                       before_column_info)
            is_same_as_after = self.get_change_result(source_field_info,
                                                      afrer_column_info)

    def get_change_result(self, source_field_info, meta_field_info):
        """
            比较接入字段与元数据是否一致
        :param source_field_info: 接入字段对象集合
        :param meta_field_info: 字段元数据对象集合
        :return: True 有不一致字段
                False 无不一致字段
        """
        if len(source_field_info) != len(meta_field_info):
            return True
        meta_field_names = [field.COL_NAME.strip().upper() for field in
                            meta_field_info]
        for i in range(0, len(source_field_info)):
            source_field = source_field_info[i]
            if not meta_field_names.__contains__(source_field.col_name):
                return True
            else:
                for j in range(0, len(meta_field_info)):
                    if meta_field_info[j].COL_NAME.upper().strip().__eq__(
                            source_field.col_name.upper().strip()):
                        if meta_field_info[j].COL_TYPE.upper().strip() != \
                                source_field.data_type.upper().strip() or \
                                        meta_field_info[j].COL_LENGTH != \
                                        source_field.col_length or \
                                        meta_field_info[j].COL_SCALE != \
                                        source_field.col_scale or \
                                        meta_field_info[j].COL_SEQ != i:
                            return True

        return False


if __name__ == '__main__':
    mata_data_service = MetaDataService("qweqew", "default", "test",
                                        "default", "test_register",
                                        "20181222", "10")
    mata_data_service.upload_meta_data()

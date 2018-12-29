# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/25
# Write By      : adtec(ZENGYU)
# Function Desc :  获取元数据信息
# History       : 2018/12/25  ZENGYU     Create
# Remarks       :
from archive.archive_enum.archive_enum import CommentChange
from archive.dao.meta_data_dao import *
from archive.model.base_model import *
from archive.model.hive_field_info import HiveFieldInfo
from utils.base_util import get_uuid
from utils.biz_excpetion import BizException
from utils.date_util import DateUtil
from utils.get_conf import get_conf_reader
from utils.hive_utils import HiveUtil
from utils.logger import Logger
from utils.str_utils import StringUtil

meta_table_info_dao = MetaTableInfoDao()
meta_table_info_his_dao = MetaTableInfoHisDao()
meta_column_info_dao = MetaColumnInfoDao()
meta_column_info_his_dao = MetaColumnInfoHisDao()

LOG = Logger()
conf_reader = get_conf_reader("conf", "base_conf.ini")
last_update_user = conf_reader.get("update_user", "update_user")
last_update_time = DateUtil.get_now_date()
common_dict = HiveUtil.get_common_dict()


class MetaDataService(object):
    """
        元数据操作类
    """

    def get_meta_field_info_list(self, table_name, data_date):
        """
            获取最近元数据字段信息
        :param table_name:
        :param data_date:
        :return:
        """
        meta_table_info = meta_table_info_dao.get_recent_meta_table_info(
            table_name, data_date)
        if not meta_table_info:
            meta_column_info = meta_column_info_dao.get_meta_data_by_table(
                meta_table_info.TABLE_ID)
            # 转换成Hive_field_info 类型
            hive_field_infos = list()
            for field in meta_column_info:
                col_seq = meta_column_info.index(field)
                hive_field_info = HiveFieldInfo(field.COL_NAME,
                                                field.COL_TYPE,
                                                field.COL_DEFAULT,
                                                field.NULL_FLAG,
                                                "No",
                                                field.DESCRIPTION,
                                                col_seq
                                                )
                hive_field_infos.append(hive_field_info)
            return hive_field_infos
        else:
            return None

    def parse_input_table(self, db_name, table_name):
        """
            解析接入表结构
        :return:
        """
        source_field_infos = []  # 获取接入数据字段列表
        cols = HiveUtil.get_table_desc(db_name,
                                       table_name)

        i = 0
        for col in cols:
            filed_info = HiveFieldInfo(col[0], col[1], col[2], col[3], col[4],
                                       col[5], i)
            source_field_infos.append(filed_info)
            i = i+1


        return source_field_infos

    def upload_meta_data(self, schema_id, source_db_name, source_table_name,
                         db_name, table_name, data_date, bucket_num):
        # type: (str, str, str, str, str, str, str) -> None

        """
            登记元数据
        :param schema_id:
        :param source_db_name: 源库名
        :param source_table_name: 源表名
        :param db_name: 目标库
        :param table_name: 目标表
        :param data_date: 执行日期
        :param bucket_num: 分桶数
        :return: void
        """
        # 检查当日是否已经登记元数据
        LOG.info("------------元数据登记检查------------")
        meta_table_info = meta_table_info_dao.get_meta_table_info_by_time(
            table_name, data_date)

        if len(meta_table_info) != 0:
            # 当日已登记元数据，需要对此元数据进行对比
            if self.check(bucket_num, meta_table_info[0]):
                LOG.debug("今日表元数据已登记 ！")
                return
            else:
                # 对表元数据进行更新
                meta_table_info_dao.update_meta_table_info(table_name,
                                                           data_date,
                                                           {
                                                               "BUCKET_NUM": bucket_num})

        try:
            LOG.info("接入表信息解析")
            source_field_info = self.parse_input_table(db_name, table_name)
            length = len(source_field_info)
            if length == 0:
                raise BizException("接入表信息解析失败！请检查接入表是否存在 ")
            LOG.info("接入表字段数为：{0}".format(length))
            self.register_meta_data(source_field_info, source_db_name,
                                    source_table_name, schema_id, table_name,
                                    data_date, bucket_num)
        except Exception as e:
            LOG.error("登记元数据失败！ ")
            LOG.error(e.message)

    def check(self, buckect_num, meta_table_info):
        """
            已登记的元数据与现元数据进行比对
        :return: True 表示一致，False表示不一致
        """
        if meta_table_info.BUCKECT_NUM == buckect_num:
            return True
        else:
            return False

    def register_meta_data(self, source_field_info, source_db_name,
                           source_table_name, schema_id,
                           table_name,
                           data_date, bucket_num):
        """
            接入表DDL 与 元数据DDL进行比对
        :return:
        """
        # 查找data_date 日期前后是否存在元数据记录

        before_table_infos = meta_table_info_dao.get_before_meta_table_infos(
            table_name, data_date)

        after_table_infos = meta_table_info_dao.get_after_meta_table_infos(
            table_name, data_date)
        source_table_comment = HiveUtil.get_table_comment(source_db_name,
                                                          source_table_name)
        if len(before_table_infos) == 0 and len(after_table_infos) == 0:
            # 说明是新的归档数据 直接登记元数据
            table_id = get_uuid()

            # 登记表元数据
            new_meta_table_info = DidpMetaTableInfo(
                TABLE_ID=table_id,
                SCHEMA_ID=schema_id,
                LAST_UPDATE_TIME=last_update_time,
                LAST_UPDATE_USER=last_update_user,
                TABLE_NAME=table_name,
                BUCKET_NUM=bucket_num,
                DESCRIPTION=source_table_comment,
                RELEASE_DATE=data_date)

            table_his_id = get_uuid()  # 表历史id

            new_meta_table_info_his = DidpMetaTableInfoHis(
                TABLE_HIS_ID=table_his_id,
                TABLE_ID=table_id,
                SCHEMA_ID=schema_id,
                LAST_UPDATE_TIME=last_update_time,
                LAST_UPDATE_USER=last_update_user,
                TABLE_NAME=table_name,
                BUCKET_NUM=bucket_num,
                DESCRIPTION=source_table_comment,
                RELEASE_DATE=data_date
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

                meta_column_info_his_dao.add_meta_column_his(
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

            # 获取table_comment
            before_table_comment = before_table_infos[0].DESCRIPTION if \
                before_table_infos[0] else ""
            after_table_comment = after_table_infos[0].DESCRIPTION if \
                after_table_infos[0] else ""
            # 调用比较方法
            is_same_as_before = (not self.get_change_result(source_field_info,
                                                            before_column_info)) and (
                                    not self.getTableCommentChangeResult(
                                        source_table_comment,
                                        before_table_comment))
            is_same_as_after = (not self.get_change_result(source_field_info,
                                                           afrer_column_info)) and (
                                   not self.getTableCommentChangeResult(
                                       source_table_comment,
                                       after_table_comment))

            before_table_info = before_table_infos[0]
            after_table_info = after_table_infos[0]
            if is_same_as_before and (not is_same_as_after):
                # 这次变化和上一次的相同，和后一次不同
                LOG.debug("无变化 ！ ")
                meta_table_info_his = meta_table_info_his_dao.get_meta_table_info_his(
                    before_table_info.TABLE_ID,
                    before_table_info.SCHEMA_ID,
                    before_table_info.RELEASE_DATE)
                if source_table_comment != "" and not before_table_comment.__eq__(
                        source_table_comment):
                    # 更新Table_comment
                    meta_table_info_dao.update_meta_table_info(table_name,
                                                               before_table_info.RELEASE_DATE,
                                                               {
                                                                   "DESCRIPTION": source_table_comment})

                    new_meta_table_info_his = DidpMetaColumnInfoHis(
                        TABLE_HIS_ID=get_uuid(),
                        TABLE_ID=before_table_info.TABLE_ID,
                        SCHEMA_ID=schema_id,
                        LAST_UPDATE_TIME=last_update_time,
                        LAST_UPDATE_USER=last_update_user,
                        TABLE_NAME=table_name,
                        BUCKET_NUM=bucket_num,
                        DESCRIPTION=source_table_comment,
                        RELEASE_DATE=before_table_info.RELEASE_DATE)
                    LOG.debug("登记表元数据历史表")
                    meta_table_info_his_dao.add_meta_table_info_his(
                        new_meta_table_info_his)
                    meta_table_info_his = new_meta_table_info_his
                    # 更新字段备注
                self.update_field_comment(source_field_info,
                                          before_column_info,
                                          meta_table_info_his)

            elif not is_same_as_before and not is_same_as_after:
                # 和前后都不同
                LOG.debug("这次变化和前后都不同 ! ")
                # 登记数据资产
                table_id = get_uuid()
                new_meta_table_info = DidpMetaTableInfo(
                    TABLE_ID=table_id,
                    SCHEMA_ID=schema_id,
                    LAST_UPDATE_TIME=last_update_time,
                    LAST_UPDATE_USER=last_update_user,
                    TABLE_NAME=table_name,
                    BUCKET_NUM=bucket_num,
                    DESCRIPTION=source_table_comment,
                    RELEASE_DATE=data_date)
                LOG.debug("登记表元数据 ")
                meta_table_info_dao.add_meta_table_info(new_meta_table_info)
                table_his_id = get_uuid()
                new_meta_table_info_his = DidpMetaTableInfoHis(
                    TABLE_HIS_ID=table_his_id,
                    TABLE_ID=table_id,
                    SCHEMA_ID=schema_id,
                    LAST_UPDATE_TIME=last_update_time,
                    LAST_UPDATE_USER=last_update_user,
                    TABLE_NAME=table_name,
                    BUCKET_NUM=bucket_num,
                    DESCRIPTION=source_table_comment,
                    RELEASE_DATE=data_date)
                meta_table_info_his_dao.add_meta_table_info_his(
                    new_meta_table_info_his)
                # 登记元数据字段
                LOG.debug("登记字段元数据")
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
                    meta_column_info_his_dao.add_meta_column_his(
                        meta_field_info_his)

            elif not is_same_as_before and is_same_as_after:
                LOG.debug("与前一次不同, 与后一次相同")
                LOG.debug("更新后一次数据的RELEASE_DATE ")
                meta_table_info_dao.update_meta_table_info(
                    after_table_info.TABLE_ID,
                    after_table_info.RELEASE_DATE,
                    {"RELEASE_DATE": data_date})
                # 添加表元数据历史表记录
                new_meta_table_info_his = DidpMetaColumnInfoHis(
                    TABLE_HIS_ID=get_uuid(),
                    TABLE_ID=after_table_info.TABLE_ID,
                    SCHEMA_ID=schema_id,
                    LAST_UPDATE_TIME=last_update_time,
                    LAST_UPDATE_USER=last_update_user,
                    TABLE_NAME=table_name,
                    BUCKET_NUM=bucket_num,
                    DESCRIPTION=source_table_comment,
                    RELEASE_DATE=data_date)
                meta_table_info_his_dao.add_meta_table_info_his(
                    new_meta_table_info_his)
                self.update_field_comment(source_field_info, afrer_column_info,
                                          new_meta_table_info_his)

    def get_change_result(self, source_field_info, meta_field_info,
                          ):
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
                # 判断接入字段是否存在于元数据表中
                return True
            else:
                for j in range(0, len(meta_field_info)):
                    if StringUtil.eq_ignore(meta_field_info[j].COL_NAME,
                                            source_field.col_name):
                        if not StringUtil.eq_ignore(meta_field_info[j].COL_TYPE,
                                                    source_field.data_type) or \
                                        meta_field_info[j].COL_LENGTH != \
                                        source_field.col_length or \
                                        meta_field_info[j].COL_SCALE != \
                                        source_field.col_scale or \
                                        meta_field_info[j].COL_SEQ != i:
                            return True

                    # 判断字段备注改变是否增加新版本
                    comment_change = common_dict.get(
                        CommentChange.FIELD_COMMENT_CHANGE_DDL.value)

                    if comment_change.PARAM_VALUE.upper().__eq__("TRUE"):
                        comment1 = source_field.comment if source_field.comment else ""
                        comment2 = meta_field_info[j].COL_DESC if \
                            meta_field_info[j].COL_DESC else ""
                        if not comment1.__eq__(comment2):
                            return True
        return False

    def getTableCommentChangeResult(self, source_table_comment,
                                    meta_table_comment):
        """
            判断表描述是否相同
        :param meta_table_comment: 元数据表描述

        :return: True： 不一致 False 一致
        """
        comment_change = common_dict.get(
            CommentChange.TABLE_COMMENT_CHANGE_DDL.value)
        if comment_change.PARAM_VALUE.upper().strip().__eq__("TRUE"):
            comment1 = source_table_comment
            comment2 = meta_table_comment if meta_table_comment else ""
            if not StringUtil.eq_ignore(comment1, comment2):
                return True
            else:
                return False

    def update_field_comment(self, entity_list, bean_list, meta_table_his):
        """
            更新字段备注
        :param entity_list: 接入字段数据对象集合
        :param bean_list: 字段元数据对象集合
        :meta_table_his: 表元数据历史对象

        :return:
        """
        comment_change = common_dict.get(
            CommentChange.FIELD_COMMENT_CHANGE_DDL.value)
        if comment_change.PARAM_VALUE.upper().__eq__("TRUE"):
            return
        for bean in bean_list:
            for entity in entity_list:
                if StringUtil.eq_ignore(bean.COL_NAME, entity.col_name):
                    if bean.DESCRIPTION is None:
                        bean.DESCRIPTION = ""
                    if entity.comment is None:
                        entity.comment = ""
                    if not StringUtil.is_blank(
                            entity.comment) and not StringUtil.eq_ignore(
                        bean.DESCRIPTION, entity.comment):
                        LOG.debug("更新DDL备注，field = {0},comment = {1}".format(
                            bean.COL_NAME, entity.comment))
                        # 更新
                        meta_column_info_dao.update_meta_column(bean.TABLE_ID,
                                                                bean.COL_NAME, {
                                                                    "DESCRIPTION": entity.comment})
                        # 插入字段元数据历史表
                        table_his_id = meta_table_his.TABLE_HIS_ID
                        table_id = meta_table_his.TABLE_ID
                        meta_column_info_his = DidpMetaColumnInfoHis(
                            TABLE_HIS_ID=table_his_id,
                            COLUMN_ID=bean.COLUMN_ID,
                            TABLE_ID=table_id,
                            LAST_UPDATE_TIME=last_update_time,
                            LAST_UPDATE_USER=last_update_user,
                            COL_SEQ=bean.COL_SEQ,
                            COL_NAME=bean.COL_NAME,
                            COL_DESC=entity.comment,
                            COL_TYPE=bean.COL_TYPE,
                            COL_LENGTH=bean.COL_LENGTH,
                            COL_SCALE=bean.COL_SCALE,
                            COL_DEFAULT=bean.COL_DEFAULT,
                            NULL_FLAG=bean.NULL_FLAG
                        )
                        meta_column_info_his_dao.add_meta_column_his(
                            meta_column_info_his)

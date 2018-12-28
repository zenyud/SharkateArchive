# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/27
# Write By      : adtec(ZENGYU)
# Function Desc :  Hive 建表, 更新表
# History       : 2018/12/27  ZENGYU     Create
# Remarks       :
from archive.archive_enum.archive_enum import AddColumn, OrgPos, PartitionKey
from archive.meta_data_service import MetaDataService
from archive.model.field_state import FieldState, MetaTypeInfo
from utils.hive_utils import HiveUtil
from utils.logger import Logger
from utils.str_utils import StringUtil

LOG = Logger()


class ManageTable(object):
    """
        Hive 表的创建与修改
    """

    @property
    def field_change_list(self):
        return self.field_change_list

    @field_change_list.setter
    def field_change_list(self, field_change_list):
        self.field_change_list = field_change_list

    @property
    def field_type_change_list(self):
        return self.field_type_change_list

    @field_type_change_list.setter
    def field_type_change_list(self, field_type_change_list):
        self.field_type_change_list = field_type_change_list

    def create_table(self, source_db_name, source_table_name, db_name,
                     table_name, org_pos, common_dict,
                     cluster_col, bucket_num):
        # type: (str,str,str, str, int, dict, str, int) -> None
        """
            创建Hive表
        :param source_db_name: 源库名
        :param source_table_name: 源表名
        :param db_name: 目标库
        :param table_name: 目标表
        :param org_pos: org 的位置
        :param common_dict: 公共dict
        :param cluster_col: 分桶键
        :param bucket_num: 分桶数
        :return:  None
        """
        HiveUtil.execute(
            "DROP TABLE {DB_NAME}.{TABLE_NAME} ".format(DB_NAME=db_name,
                                                        TABLE_NAME=table_name))
        # 获取增加字段
        col_date = common_dict.get(AddColumn.COL_DATE.value)
        execute_sql = "CREATE TABLE {DB_NAME}.{TABLE_NAME} ( {COL_DATE} varchar(10), ".format(
            DB_NAME=db_name,
            TABLE_NAME=table_name,
            COL_DATE=col_date
        )  # 原始执行Sql

        # org_pos  1-没有机构字段 2-字段在列中 3-字段在分区中
        if int(org_pos) == OrgPos.COLUMN.value:
            execute_sql = execute_sql + "{ORG_COL} string ,".format(
                ORG_COL=common_dict.get(AddColumn.COL_ORG.value))
            # print common_dict.get(AddColumn.COL_ORG.value)
        # 组装字段

        body = self.create_table_body(source_db_name, source_table_name, False)
        execute_sql = execute_sql + body + ")"

        if int(org_pos) == OrgPos.PARTITION.value:
            # 机构字段在分区中
            execute_sql = execute_sql + " PARTITIONED BY ({org_col} string)".format(
                org_col=common_dict.get(PartitionKey.ORG.value))

        # 默认全部为事务表
        execute_sql = execute_sql + " CLUSTERED  BY ({CLUSTER_COL}) " \
                                    "INTO {BUCKET_NUM} BUCKETS STORED AS ORC " \
                                    "tblproperties('orc.compress'='SNAPPY' ," \
                                    "'transactional'='true')".format(
            CLUSTER_COL=cluster_col,
            BUCKET_NUM=bucket_num)
        LOG.debug("建表语句为：%s " % execute_sql)
        HiveUtil.execute(execute_sql)

    def create_table_body(self, db_name, table_name, is_temp_table):
        """
            构建表的body
        :param db_name: 库名
        :param table_name: 表名
        :param is_temp_table: 是否是临时表
        :return: sql str
        """
        source_fields = HiveUtil.get_table_desc(db_name,
                                                table_name)
        sql = ""
        if is_temp_table:
            for field in source_fields:
                sql = sql + "{field_name} string,".format(field_name=field[0])
        else:
            for field in source_fields:
                sql = sql + "{field_name} {field_type} ".format(
                    field_name=field[0],
                    field_type=field[1])
                if not StringUtil.is_blank(field[5]):
                    # 看是否有字段备注
                    sql = sql + "comment '{comment_content}'".format(
                        comment_content=field[5])
                sql = sql + ","
        return sql[:-1]

    def change_table_columns(self, db_name, table_name, data_date):
        """
            根据表结构变化增加新的字段
        :return:
        """
        change_detail_buffer = ""
        alter_sql = ""

        self.get_fields_rank_list(db_name, table_name, data_date)

        if not self.field_change_list:
            # 有字段的变化
            for field in self.field_change_list:
                # field type : class FieldState
                if field.hive_no == -2:
                    change_detail_buffer = change_detail_buffer \
                                           + " `{field_name}`  {field_type},". \
                                               format(field_name=field.field,
                                                      field_type=field.type)

        if len(change_detail_buffer) > 0:
            change_detail_buffer = change_detail_buffer[:-1]  # 去掉末尾的逗号
            alter_sql = "alter table {db_name}.{table_name} add columns( {buffer} )".format(
                db_name=db_name,
                table_name=table_name,
                buffer=change_detail_buffer)
            HiveUtil.execute(alter_sql)
        alter_sql2 = ""
        if not self.field_type_change_list:
            # 有字段类型改变
            for field in self.field_type_change_list:
                alter_sql2 = alter_sql2 + "alter table {db_name}.{table_name} " \
                                          "change column `{column}` `{column}` {type} ". \
                    format(db_name=db_name,
                           table_name=table_name,
                           column=field.field,
                           type=field.type)
                if not StringUtil.is_blank(field.comment_ddl):
                    # 若备注不为空 则添加备注
                    alter_sql2 = alter_sql2 + " comment '{comment}' ".format(
                        comment=field.comment_ddl)
                HiveUtil.execute(alter_sql2)

    def get_fields_rank_list(self, db_name, table_name, data_date):
        """
            获取归档数据字段排列结果，将历史元数据信息与现有的HIVE元数据对照比较返回比较结果
        :param db_name:
        :param table_name:
        :param data_date:
        :return:
        """
        meta_field_infos = MetaDataService().get_meta_field_info_list(
            table_name, data_date)  # 元数据表中的字段信息
        # Hive 中的字段信息
        hive_field_infos = HiveUtil.get_hive_meta_field(db_name, table_name,
                                                        True)
        # 字段名更改列表
        self.field_change_list = self.get_change_list(meta_field_infos,
                                                      hive_field_infos)

        # 检查 字段类型是否改变
        self.field_type_change_list = self.check_column_modify()

    def check_column_modify(self):
        """
            检查字段类型是否改变
        """
        change_fields = set()
        for field in self.field_change_list:

            meta_type_hive = field.hive_type
            meta_type_ddl = field.ddl_type
            if meta_type_ddl is None or meta_type_ddl is None:
                # 新增字段 跳过
                continue
            if not meta_type_ddl.__eq__(meta_type_hive):
                # 字段类型不同,判断有哪些不同
                if StringUtil.eq_ignore(meta_type_ddl.field_type,
                                        meta_type_hive.field_type):
                    # 类型相同判断精度,允许decimal字段精度扩大
                    if meta_type_hive.field_length < meta_type_ddl.field_length \
                        and meta_type_hive.field_scale < meta_type_ddl.field_scale:
                        LOG.debug("字段{field_name}精度扩大")

    # noinspection PyArgumentList
    def get_change_list(self, meta_field_infos, hive_field_infos):
        """
            获取所有字段
        :param meta_field_infos: 原表
        :param hive_field_infos: hive表
        :return:
        """
        hive_field_name_list = [field.col_name.upper() for field in
                                hive_field_infos]
        meta_field_name_list = [field.COL_NAME.upper() for field in
                                meta_field_infos]

        # 进行对比0
        field_change_list = list()
        for hive_field in hive_field_infos:
            hive_no = hive_field.col_seq  # 字段序号
            meta_current_no = -1
            meta_index = -1
            if meta_field_name_list.__contains__(hive_field.col_name.upper()):
                meta_index = meta_field_name_list.index(
                    hive_field.col_name.upper())
                meta_current_no = meta_field_infos[meta_index].COL_SEQ
                ddl_type = meta_field_infos[meta_index].COL_TYPE
                meta_comment = meta_field_infos[meta_index].DESCRIPTION
            else:
                # 源数据中没有Hive的信息
                meta_current_no = -1
                hive_no = -1  # Hive 中有,元数据没有的字段
                ddl_type = None
                meta_comment = None
            hive_data_type = MetaTypeInfo(hive_field.col_type,
                                          hive_field.col_length,
                                          hive_field.col_scale)
            if meta_index == -1:
                # 元数据中没有Hive的字段
                ddl_data_type = None
            else:
                ddl_data_type = MetaTypeInfo(
                    meta_field_infos[meta_index].COL_TYPE,
                    meta_field_infos[meta_index].COL_LENGTH,
                    meta_field_infos[meta_index].COL_SCALE
                )
            field_state = FieldState(hive_field.col_name.upper(),
                                     hive_field.col_seq,
                                     meta_current_no,
                                     ddl_data_type,
                                     hive_data_type,
                                     hive_field.comment,
                                     meta_comment,
                                     hive_no)

            field_change_list.append(field_state)

        for meta_field in meta_field_infos:
            if not hive_field_name_list.__contains__(
                    meta_field.COL_NAME.upper()):
                # Hive 里不包含 元数据中的字段
                ddl_data_type = MetaTypeInfo(meta_field.COL_TYPE,
                                             meta_field.COL_LENGTH,
                                             meta_field.COL_SCALE)

                field_state = FieldState(meta_field.COL_NAME.upper(),
                                         -1,
                                         meta_field.COL_SEQ,
                                         ddl_data_type,
                                         None,
                                         None,
                                         meta_field.DESCRIPTION,
                                         -2
                                         )

                field_change_list.append(field_state)

        change = False  # 判断是否有字段改变 False 无变化 True 有变化
        for field in field_change_list:
            if field.hive_no < 0 or field.full_seq != field.current_seq:
                change = True
        # 如果没有改变 则将field_change_list 置空
        if not change:
            field_change_list = None

        return field_change_list


if __name__ == '__main__':
    pass

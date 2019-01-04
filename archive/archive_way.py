# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 归档方式
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
import argparse
import time
from abc import ABCMeta
import abc
from archive.archive_enum.archive_enum import DatePartitionRange, AddColumn, \
    OrgPos, PartitionKey

from archive.model.field_state import FieldState
from archive.model.hive_field_info import MetaTypeInfo
from archive.service.hds_struct_control import HdsStructControl
from archive.service.meta_data_service import MetaDataService
from utils.biz_excpetion import BizException
from utils.date_util import DateUtil
from utils.hive_utils import HiveUtil
from utils.logger import Logger
from utils.str_utils import StringUtil

LOG = Logger()


class ArchiveData(object):
    __metaclass__ = ABCMeta

    """数据归档类
        args:参数
    """
    lock_archive = False  # 归档锁
    lock_stat = False  # 元数据锁
    hds_struct_control = HdsStructControl()
    meta_data_service = MetaDataService()
    common_dict = {}  # 公共代码字典
    data_scope = ""
    start_date = ""
    end_date = ""
    field_change_list = None  # 变更字段列表
    field_type_change_list = None  # 变更字段类型列表
    source_ddl = None  # 原始表DDL列表
    source_count = 0  # 原始表数据量
    archive_count = 0  # 归档数据条数
    data_asset_infos = None  # 当日的数据资产列表

    def __init__(self):
        self.__args = self.archive_init()

        self.__print_arguments()

    @property
    def args(self):
        return self.__args

    def init_common_dict(self):
        common_dict = HiveUtil.get_common_dict()
        if len(common_dict) == 0:
            raise BizException("初始化公共代码失败！请检查数据库")
        else:
            self.common_dict = common_dict

    def __print_arguments(self):
        """ 参数格式化输出

        Args:

        Returns:

        Raise:

        """

        LOG.debug("-------------------参数清单-------------------")
        LOG.debug("数据对象名       : {0}".format(self.__args.obj))
        LOG.debug("SCHEMA ID       : {0}".format(self.__args.schemaID))
        LOG.debug("机构号           : {0}".format(self.__args.org))
        LOG.debug("源数据增全量      : {0}".format(self.__args.sourceDataMode))
        LOG.debug("源库名           : {0}".format(self.__args.sourceDbName))
        LOG.debug("源表名           : {0}".format(self.__args.sourceTableName))
        LOG.debug("过滤条件         : {0}".format(self.__args.filterSql))
        LOG.debug("过滤字段         : {0}".format(self.__args.filterCol))
        LOG.debug("归档库名          : {0}".format(self.__args.dbName))
        LOG.debug("归档表名          : {0}".format(self.__args.tableName))
        LOG.debug("归档方式           : {0}".format(self.__args.saveMode))
        LOG.debug("数据日期           : {0}".format(self.__args.dataDate))
        LOG.debug("日期分区范围        : {0}".format(self.__args.dateRange))
        LOG.debug("机构字段位置        : {0}".format(self.__args.orgPos))
        LOG.debug("分桶键             : {0}".format(self.__args.clusterCol))
        LOG.debug("分桶数             : {0}".format(self.__args.bucketsNum))
        LOG.debug("全量历史表名        : {0}".format(self.__args.allTableName))
        LOG.debug("增量历史表名        : {0}".format(self.__args.addTableName))
        LOG.debug("----------------------------------------------")

    @staticmethod
    def archive_init():
        """
            参数初始化
        :return:
        """
        # 参数解析
        parser = argparse.ArgumentParser(description="数据归档组件")

        parser.add_argument("-obj", required=True, help="数据对象名")
        parser.add_argument("-org", required=True, help="机构")
        parser.add_argument("-sourceDataMode", required=True,
                            help="源数据增全量（1-全量 2-增量）")
        parser.add_argument("-sourceDbName", required=True, help="源库名")
        parser.add_argument("-sourceTableName", required=True, help="源表名")
        parser.add_argument("-filterSql", required=False,
                            help="采集过滤SQL条件（WHERE 后面部分）")
        parser.add_argument("-filterCol", required=False, help="过滤字段")
        parser.add_argument("-schemaID", required=True, help="取连接信息")
        parser.add_argument("-dbName", required=True, help="归档库名")
        parser.add_argument("-tableName", required=True, help="归档表名")
        parser.add_argument("-saveMode", required=True,
                            help="归档方式（1-历史全量、2-历史增量、"
                                 "4-拉链、5-最近一天增量、6-最近一天全量）")
        parser.add_argument("-dataDate", required=True,
                            help="数据日期（yyyymmdd）")
        parser.add_argument("-dateRange", required=True,
                            help="日期分区范围（N-不分区、M-月、Q-季、Y-年）")
        parser.add_argument("-orgPos", required=True,
                            help="机构字段位置（1-没有机构字段 "
                                 "2-字段在列中 3-字段在分区中）")
        parser.add_argument("-clusterCol", required=True, help="分桶键")
        parser.add_argument("-bucketsNum", required=True, help="分桶数")
        parser.add_argument("-allTableName", required=False,
                            help="全量历史表名（当归档方式为历史增量，"
                                 "且源数据为全量时传入），格式 dbname.tablename")
        parser.add_argument("-addTableName", required=False,
                            help="增量历史表名（当归档方式为历史全量，"
                                 "且源数据为增量时传入），格式 dbname.tablename")
        args = parser.parse_args()
        return args

    def lock(self):
        """
            归档任务锁
        :return:
        """
        if self.hds_struct_control.find_archive(self.__args.obj,
                                                self.__args.org) is None:
            try:
                self.hds_struct_control.archive_lock(self.__args.obj,
                                                     self.__args.org)
                self.lock_archive = True
            except Exception as e:
                raise BizException(
                    "待归档表有另外正在归档的任务或后台数据库更新错，请稍后再试。[{0}]".format(e.message))
        else:
            raise BizException("待归档表有另外正在归档的任务，请稍后再试 ")

    def data_partition_check(self):
        """
            日期分区字段检查
        :return:
        """
        if HiveUtil.exist_table(self.__args.dbName, self.__args.tableName):
            if self.__args.dateRange == DatePartitionRange.ALL_IN_ONE.value \
                    and HiveUtil.has_partition(self.__args.dbName,
                                               self.__args.tableName):
                raise BizException("归档日期分区与Hive表不一致 ！！！")

        def set_value(x, y, z):
            """ 赋值 """
            return x, y, z

        switch = {
            DatePartitionRange.MONTH.value: set_value(
                self.__args.dataDate[0:6],
                DateUtil().get_month_start(
                    self.__args.dataDate),
                DateUtil().get_month_end(
                    self.__args.dataDate)),

            DatePartitionRange.QUARTER_YEAR.value: set_value(
                DateUtil().get_quarter(self.__args.dataDate),
                DateUtil().get_month_start(
                    self.__args.dataDate),
                DateUtil().get_month_end(
                    self.__args.dataDate)),

            DatePartitionRange.YEAR.value: set_value(self.__args.dataDate[0:4],
                                                     DateUtil().get_year_start(
                                                         self.__args.dataDate),
                                                     DateUtil().get_year_end(
                                                         self.__args.dataDate)),
            DatePartitionRange.ALL_IN_ONE.value: ("", "", "")
        }
        # 获取分区范围，开始日期，结束日期
        x, y, z = switch[self.__args.dateRange]
        self.data_scope = x
        self.start_date = y
        self.end_date = z

    def org_check(self):
        if HiveUtil.exist_table(self.__args.dbName, self.__args.tableName):
            if int(self.__args.orgPos) != HiveUtil.get_org_pos(
                    self.__args.dbName,
                    self.__args.tableName):
                raise BizException("归档机构分区与hive表中不一致 !!!")

    def meta_lock(self):
        """
            元数据处理加锁
        :return:
        """
        start_time = time.time()
        self.lock_stat = False
        self.meta_lock_do()
        while not self.lock_stat:
            if time.time() - start_time > 60000:
                raise BizException("元数据更新等待超时,请稍后再试！")
            try:
                time.sleep(1)
            except Exception as e:
                LOG.debug(e)
            self.meta_lock_do()

    def meta_lock_do(self):

        if not self.hds_struct_control.meta_lock_find(self.__args.obj,
                                                      self.__args.org):
            try:
                self.hds_struct_control.meta_lock(self.__args.obj,
                                                  self.__args.org)

                self.lock_stat = True
            except Exception as e:
                LOG.debug("元数据更新队列等待中 。。。 ")

    def meta_unlock(self):
        """
                   元数据锁解除
               :return:
               """
        if self.lock_stat:
            self.hds_struct_control.meta_unlock(self.__args.obj,
                                                self.__args.org)
            self.lock_stat = False

    def upload_meta_data(self):
        """
            登记元数据
        :return:
        """
        self.meta_data_service.upload_meta_data(self.__args.schemaID,
                                                self.__args.sourceDbName,
                                                self.__args.sourceTableName,
                                                self.__args.dbName,
                                                self.__args.tableName,
                                                self.__args.dataDate,
                                                self.__args.bucketsNum)

    @abc.abstractmethod
    def create_table(self):
        """
            创建表
        :return:
        """

    def change_table_columns(self):
        """
                   根据表结构变化增加新的字段
               :return:
               """
        change_detail_buffer = ""
        alter_sql = ""

        self.get_fields_rank_list(self.__args.dbName, self.__args.tableName,
                                  self.__args.dataDate)

        if self.field_change_list:
            # 有字段的变化
            for field in self.field_change_list:
                # field type : class FieldState
                if field.hive_no == -2:
                    # hive 需要新增字段
                    change_detail_buffer = change_detail_buffer \
                                           + " `{field_name}`  {field_type},". \
                                               format(field_name=field.field_name,
                                                      field_type=field.ddl_type.get_whole_type)
                    LOG.debug("field length %s " % field.ddl_type.field_length)
                    LOG.debug("whole_type %s " % field.ddl_type.get_whole_type)
        if len(change_detail_buffer) > 0:
            change_detail_buffer = change_detail_buffer[:-1]  # 去掉末尾的逗号
            alter_sql = "ALTER TABLE {db_name}.{table_name} ADD COLUMNS ({buffer}) ".format(
                db_name=self.__args.dbName,
                table_name=self.__args.tableName,
                buffer=change_detail_buffer)

            LOG.debug("sql 语句为 %s" % alter_sql)
            HiveUtil.execute(alter_sql)
        alter_sql2 = ""
        if self.field_type_change_list:
            # 有字段类型改变
            for field in self.field_type_change_list:
                alter_sql2 = alter_sql2 + "alter table {db_name}.{table_name} " \
                                          "change column `{column}` `{column}` {type} ". \
                    format(db_name=self.__args.dbName,
                           table_name=self.__args.tableName,
                           column=field.field_name,
                           type=field.ddl_type.get_whole_type)
                LOG.debug("field length %s " % field.ddl_type.field_length)
                if not StringUtil.is_blank(field.comment_ddl):
                    # 若备注不为空 则添加备注
                    alter_sql2 = alter_sql2 + " comment '{comment}' ".format(
                        comment=field.comment_ddl)

                LOG.debug("修改表sql为：%s" % alter_sql2)
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

    @staticmethod
    def get_change_list(meta_field_infos, hive_field_infos):
        """
            获取所有字段
        :param meta_field_infos: 接入表元数据
        :param hive_field_infos: 现有的hive表
        :return:
        """
        hive_field_name_list = [field.col_name.upper() for field in
                                hive_field_infos]
        meta_field_name_list = [x.col_name.upper() for x in
                                meta_field_infos]
        LOG.debug("Hive字段个数为：%s" % len(hive_field_name_list))
        LOG.debug("接入数据字段个数为：%s" %len(meta_field_name_list) )
        # 进行对比0
        field_change_list = list()
        for hive_field in hive_field_infos:
            hive_no = hive_field.col_seq  # 字段序号

            if hive_field.col_name.upper() in meta_field_name_list:
                meta_index = meta_field_name_list.index(
                    hive_field.col_name.upper())
                meta_current_no = meta_field_infos[meta_index].col_seq
                ddl_type = MetaTypeInfo(
                    meta_field_infos[meta_index].data_type,
                    meta_field_infos[meta_index].col_length,
                    meta_field_infos[meta_index].col_scale)
                meta_comment = meta_field_infos[meta_index].comment
            else:
                # 源数据中没有Hive的信息
                meta_current_no = -1
                hive_no = -1  # Hive 中有,元数据没有的字段
                ddl_type = None
                meta_comment = None

            hive_type = MetaTypeInfo(hive_field.data_type,
                                     hive_field.col_length,
                                     hive_field.col_scale)

            field_state = FieldState(hive_field.col_name.upper(),
                                     hive_field.col_seq,
                                     meta_current_no,
                                     ddl_type,
                                     hive_type,
                                     hive_field.comment,
                                     meta_comment,
                                     hive_no)

            field_change_list.append(field_state)

        for meta_field in meta_field_infos:
            if meta_field.col_name.upper() not in hive_field_name_list:
                # Hive 里不包含 元数据中的字段
                LOG.debug("meta_field.col_length : %s " % meta_field.col_length)
                ddl_data_type = MetaTypeInfo(meta_field.data_type,
                                             meta_field.col_length,
                                             meta_field.col_scale)

                field_state = FieldState(meta_field.col_name.upper(),
                                         -1,
                                         meta_field.col_seq,
                                         ddl_data_type,
                                         None,
                                         None,
                                         meta_field.comment,
                                         -2  # hive中需要新增
                                         )

                field_change_list.append(field_state)

        change = False  # 判断是否有字段改变 False 无变化 True 有变化
        for field in field_change_list:
            if field.hive_no < 0 or StringUtil.eq_ignore(field.full_seq,
                                                         field.current_seq):
                change = True
        # 如果没有改变 则将field_change_list 置空

        if not change:
            field_change_list = None
        for f in field_change_list:
            LOG.info(
                "filed 字段信息：filed_name:{field_name}|"
                "ddl_type:{ddl_type}|hive_type:{hive_type}|hive_no:{hive_no}"
                    .format(field_name=f.field_name,
                            ddl_type=f.ddl_type,
                            hive_type=f.hive_type,
                            hive_no=f.hive_no))
        return field_change_list

    def check_column_modify(self):
        """
            检查字段类型是否改变
        """
        is_error = False
        change_fields = set()  # 存放变更的字段
        if self.field_change_list is not None:
            for field in self.field_change_list:

                meta_type_hive = field.hive_type
                meta_type_ddl = field.ddl_type
                if not meta_type_hive or not meta_type_ddl:
                    # 新增字段 跳过
                    continue
                if not meta_type_ddl.__eq__(meta_type_hive):
                    # 字段类型不同,判断有哪些不同
                    LOG.debug("meta_type_ddl %s " % meta_type_ddl)
                    LOG.debug("meta_type_hive %s " % meta_type_hive)
                    if StringUtil.eq_ignore(meta_type_ddl.field_type,
                                            meta_type_hive.field_type):
                        # 类型相同判断精度,允许decimal字段精度扩大

                        if meta_type_hive.field_length < meta_type_ddl.field_length \
                                and meta_type_hive.field_scale < meta_type_ddl.field_scale:
                            LOG.debug(
                                "字段{field_name}精度扩大 {hive_type} -->> {ddl_type} \n "
                                "修改表字段精度为 {ddl_type} ".format(
                                    field_name=field.field_name,
                                    hive_type=meta_type_hive.get_whole_type,
                                    ddl_type=meta_type_ddl.get_whole_type))
                            change_fields.add(field)
                            continue

                        elif meta_type_hive.field_length >= meta_type_ddl.field_length \
                                and meta_type_hive.field_scale < meta_type_ddl.field_scale:

                            old_type = meta_type_ddl.get_whole_type
                            meta_type_ddl.field_length = meta_type_hive.field_length
                            LOG.debug(
                                "字段{field_name}精度扩大 {hive_type} -->> {ddl_type1} \n "
                                "修改表字段精度为 {ddl_type2}".format(
                                    field_name=field.field_name,
                                    hive_type=meta_type_hive.get_whole_type,
                                    ddl_type1=old_type,
                                    ddl_type2=meta_type_ddl.get_whole_type
                                ))
                            continue
                        elif meta_type_hive.field_length < meta_type_ddl.field_length \
                                and meta_type_hive.field_scale > meta_type_ddl.field_scale:
                            old_type = meta_type_ddl.get_whole_type
                            meta_type_ddl.field_scale = meta_type_hive.field_scale
                            LOG.debug(
                                "字段{field_name}精度扩大 {hive_type} -->> {ddl_type1} \n "
                                "修改表字段精度为 {ddl_type2}".format(
                                    field_name=field.field_name,
                                    hive_type=meta_type_hive.get_whole_type,
                                    ddl_type1=old_type,
                                    ddl_type2=meta_type_ddl.get_whole_type
                                ))
                            continue
                        else:
                            LOG.debug(
                                "字段{field_name} 精度缩小 {hive_type}-->> {ddl_type}\n"
                                "不修改归档表字段精度 ！".format(
                                    field_name=field.field_name,
                                    hive_type=meta_type_hive.get_whole_type,
                                    ddl_type=meta_type_ddl.get_whole_type))
                    else:
                        # 不允许字段类型发生改变
                        is_error = True
                        LOG.error(
                            "字段{field_name} 类型发送变化{hive_type} -->> {ddl_type} !".format(
                                field_name=field.field_name,
                                hive_type=meta_type_hive.get_whole_type,
                                ddl_type=meta_type_ddl.get_whole_type))
                if is_error:
                    raise BizException("源数据字段类型变化过大，请处理后重新归档")

                # 判断字段备注的改变
                if not StringUtil.is_blank(field.comment_ddl):
                    # ddl 中存在备注
                    if StringUtil.is_blank(field.comment_hive):
                        # hive 没有备注
                        LOG.debug("字段{field} 备注发送变化 -->> {comment}".format(
                            field=field.field_name,
                            comment=field.comment_ddl))
                        change_fields.add(field)
                    elif not StringUtil.eq_ignore(field.comment_ddl,
                                                  field.comment_hive):
                        LOG.debug(
                            "字段{field} 备注发生变化: {comment1} -->> {comment2} ".format(
                                field=field.field_name,
                                comment1=field.comment_hive,
                                comment2=field.comment_ddl))
                        change_fields.add(field)

            ret_fields = list()
            for field in change_fields:
                ret_fields.append(field)
            return ret_fields

    @abc.abstractmethod
    def load_data(self):
        """
            装载数据
        :return:
        """
        pass

    def create_partition_sql(self, partition_range, data_scope, org):
        """
        构建表partition分区
        :param partition_range: 日期分区范围
        :param data_scope: 日期区间
        :param org: 机构分区
        :return:
        """
        s = ""
        result = ""
        if not StringUtil.eq_ignore(partition_range,
                                    DatePartitionRange.ALL_IN_ONE.value):
            s = s + "{data_scope_name} = '{data_scope}' ,".format(
                data_scope_name=self.common_dict.get(
                    PartitionKey.DATE_SCOPE.value),
                data_scope=data_scope)
        if self.__args.orgPos == OrgPos.PARTITION.value:
            s = s + "{partition_key_name} = '{partition_key}' ,".format(
                partition_key_name=self.common_dict.get(PartitionKey.ORG.value),
                partition_key=org
            )
        if len(s) > 0:
            result = "partition ({str})".format(str=s[:-1])

        return result

    def create_where_sql(self, table_alias, data_date, data_partition_range,
                         date_scope, org_pos, org):
        """
            构建表(日期分区+机构)条件语句
        :param table_alias: 表别名
        :param data_date: 数据日期
        :param data_partition_range: 分区范围
        :param date_scope: 日期分区
        :param org_pos: 机构字段位置
        :param org: 机构
        :return:  sql str
        """
        tmp_str = ""
        if StringUtil.is_blank(table_alias):
            table_alias = ""
        else:
            table_alias = table_alias + '.'
        if not StringUtil.is_blank(data_date):
            tmp_str = tmp_str + "{col_date} = '{value}'".format(
                col_date=self.common_dict.get(AddColumn.COL_DATE.value),
                value=data_date)

        if not StringUtil.eq_ignore(DatePartitionRange.ALL_IN_ONE.value,
                                    data_partition_range):
            if len(tmp_str) > 0:
                tmp_str = tmp_str + "and "
            tmp_str = tmp_str + "{date_scope} = '{value}'".format(
                date_scope=self.common_dict.get(PartitionKey.DATE_SCOPE.value),
                value=date_scope
            )

        if len(tmp_str) > 0:
            tmp_str = tmp_str + "and "
        if not StringUtil.eq_ignore(OrgPos.NONE, org_pos):
            col_name = ""
            if StringUtil.eq_ignore(OrgPos.PARTITION.value, org_pos):
                col_name = self.common_dict(PartitionKey.ORG.value)
            elif StringUtil.eq_ignore(OrgPos.COLUMN.value, org_pos):
                col_name = self.common_dict.get(AddColumn.COL_ORG.value)
            tmp_str = tmp_str + "{table_alias}.{col_name} = '{col_value}'".format(
                table_alias=table_alias,
                col_name=col_name,
                col_value=org)
        if len(tmp_str) == 0:
            return "1==1"
        else:
            return tmp_str

    def build_load_column_sql(self, table_alias, need_trim):
        """
            构建column字段sql
        :param table_alias: 表别名
        :param need_trim:
        :return:
        """
        sql = ""
        self.source_ddl = self.meta_data_service.get_meta_field_info_list(
            self.args.tableName, self.args.dataDate)
        if self.field_change_list:
            # 如果字段有变化
            for field in self.field_change_list :
                is_exists = False
                for ddl_field in self.source_ddl:
                    if StringUtil.eq_ignore(ddl_field.col_name, field.field_name):
                        is_exists = True
                        break
                if is_exists:
                    LOG.debug(field.field_name)
                    LOG.debug(field.ddl_type)
                    if self.field_change_list.index(field) == 0:
                        sql = sql + self.build_column(table_alias, field.field_name,
                                                      field.ddl_type.field_type,
                                                      need_trim)

                    else:
                        sql = sql + "," + self.build_column(table_alias,
                                                            field.field_name,
                                                            field.ddl_type.field_type,
                                                            need_trim)
                else:
                    sql = sql + ",''"
        else:
            # 无字段变化的情况
            for field in self.source_ddl:
                if self.source_ddl.index(field) == 0:
                    sql = sql + self.build_column(table_alias, field.col_name,
                                                  field.data_type,
                                                  need_trim)
                else:
                    sql = sql + "," + self.build_column(table_alias,
                                                        field.col_name,
                                                        field.data_type,
                                                        need_trim)

        return sql

    @staticmethod
    def build_column(table_alias, col_name, col_type, need_trim):
        """
        :param table_alias:
        :param col_name:
        :param col_type:
        :param need_trim:
        :return:
        """
        result = ""
        if not col_name[0].__eq__("`"):
            col_name = "`" + col_name + "`"
        if StringUtil.is_blank(table_alias):
            result = col_name
        else:
            result = table_alias + "." + col_name
        if need_trim:
            #  如果类型不为string 做trim操作
            if not col_type.upper() in ["STRING", "VARCHAR", "CHAR"]:
                result = "trim({value})".format(value=result)

        return result

    @abc.abstractmethod
    def count_archive_data(self):
        """
            统计归档条数
        :return:
        """

    def delete_exists_archive(self):
        pass

    def register_archive(self):
        pass

    def unlock(self):
        self.hds_struct_control.archive_unlock(self.__args.obj, self.__args.org)

    def run(self):
        """
        归档程序运行入口
        :return:
         1 - 成功 0 - 失败
        """
        LOG.info("判断是否有在进行的任务,并加锁 ")
        self.lock()

        LOG.info("初始化公共代码字典")
        self.init_common_dict()

        LOG.info("日期分区字段检查 ")
        self.data_partition_check()

        LOG.info("机构字段字段检查 ")
        self.org_check()

        LOG.info("元数据处理、表并发处理")
        self.meta_lock()

        LOG.info("元数据登记与更新")
        self.upload_meta_data()

        if not HiveUtil.exist_table(self.__args.dbName,
                                    self.__args.tableName):
            self.create_table()

        LOG.debug("根据表定义变化信息更新表结构 ")
        self.change_table_columns()

        LOG.info("元数据并发解锁")
        self.meta_unlock()
        LOG.info("源数据的数据量统计")
        sql = "SELECT COUNT(1) FROM {db_name}.{table_name} ".format(
            db_name=self.__args.sourceDbName,
            table_name=self.__args.sourceTableName)
        self.source_count = int(HiveUtil.execute_sql(sql)[0][0])

        LOG.debug("接入元数据的数据条数为：{0}".format(self.source_count))
        if self.source_count > 0:
            # 原始数据不为空
            LOG.info("数据载入")
            self.load_data()
            LOG.info("统计入库条数")
            self.archive_count = self.count_archive_data()
            LOG.info("入库的条数为{0}".format(self.archive_count))
        else:
            LOG.debug("归档数据为空！ ")
        LOG.info("删除已经存在的数据资产！")
        self.delete_exists_archive()
        LOG.info("登记数据资产")
        self.register_archive()

        LOG.info("解除并发锁")
        self.unlock()


class LastAddArchive(ArchiveData):
    """
        最近增量归档
    """

    def count_archive_data(self):

        hql = "select count(1) from {db_name}.{table_name} ".format(
            db_name=self.args.dbName,
            table_name=self.args.tableName)
        r = HiveUtil.execute_sql(hql)
        return int(r[0][0])

    def create_table(self):
        """
            创建Hive表
        :return:  None
        """
        HiveUtil.execute(
            "DROP TABLE {DB_NAME}.{TABLE_NAME} ".format(
                DB_NAME=self.args.dbName,
                TABLE_NAME=self.args.tableName))
        # 获取增加字段
        col_date = self.common_dict.get(AddColumn.COL_DATE.value)
        execute_sql = "CREATE TABLE {DB_NAME}.{TABLE_NAME} ( {COL_DATE} varchar(10), ".format(
            DB_NAME=self.args.dbName,
            TABLE_NAME=self.args.tableName,
            COL_DATE=col_date
        )  # 原始执行Sql

        # org_pos  1-没有机构字段 2-字段在列中 3-字段在分区中
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            execute_sql = execute_sql + "{ORG_COL} string ,".format(
                ORG_COL=self.common_dict.get(AddColumn.COL_ORG.value))
            # print common_dict.get(AddColumn.COL_ORG.value)
        # 组装字段

        body = self.create_table_body(self.args.sourceDbName,
                                      self.args.sourceTableName, False)
        execute_sql = execute_sql + body + ")"

        if int(self.args.orgPos) == OrgPos.PARTITION.value:
            # 机构字段在分区中
            execute_sql = execute_sql + " PARTITIONED BY ({org_col} string)".format(
                org_col=self.common_dict.get(PartitionKey.ORG.value))

        # 默认全部为事务表
        execute_sql = execute_sql + " CLUSTERED  BY ({CLUSTER_COL}) " \
                                    "INTO {BUCKET_NUM} BUCKETS STORED AS ORC " \
                                    "tblproperties('orc.compress'='SNAPPY' ," \
                                    "'transactional'='true')".format(
            CLUSTER_COL=self.args.clusterCol,
            BUCKET_NUM=self.args.bucketsNum)
        LOG.debug("建表语句为：%s " % execute_sql)
        HiveUtil.execute(execute_sql)

    @staticmethod
    def create_table_body(db_name, table_name, is_temp_table):
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

    def load_data(self):
        pre_table_name = self.args.sourceDbName + "." + self.args.sourceTableName
        hql = ""

        # 清空表数据
        hql = "TRUNCATE TABLE {DB_NAME}.{TABLE_NAME}".format(
            DB_NAME=self.args.dbName,
            TABLE_NAME=self.args.tableName)
        HiveUtil.execute(hql)
        # 插入数据

        hql = ""
        hql = "from {source_db_name}.{source_table_name} " \
              "insert into table {db_name}.{table_name} {partition_sql}" \
              "select '{data_date}',".format(
            source_db_name=self.args.sourceDbName,
            source_table_name=self.args.sourceTableName,
            db_name=self.args.dbName,
            table_name=self.args.tableName,
            partition_sql=self.create_partition_sql(self.args.dateRange,
                                                    self.data_scope,
                                                    self.args.org),
            data_date=self.args.dataDate
        )
        if int(self.args.orgPos) == OrgPos.COLUMN.value:
            hql = hql + " '%s' ," % self.args.org

        # 构造字段的sql
        hql = hql + self.build_load_column_sql("", True)
        LOG.debug("构造的hql是{0}".format(hql))
        HiveUtil.execute(hql)
        self.archive_count = self.count_archive_data()

        pass


class LastAllArchive(ArchiveData):
    def count_archive_data(self):
        pass

    def create_table(self):
        pass

    def load_data(self):
        pass


class AddArchive(ArchiveData):
    def count_archive_data(self):
        pass

    def create_table(self):
        pass

    def load_data(self):
        pass


class AllArchive(ArchiveData):
    def count_archive_data(self):
        pass

    def create_table(self):
        pass

    def load_data(self):
        pass


class ChainTransArchive(ArchiveData):
    def count_archive_data(self):
        pass

    def create_table(self):
        pass

    def load_data(self):
        pass

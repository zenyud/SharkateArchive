# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 日期检查
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from archive.archive_enum.archive_enum import DatePartitionRange
from utils.biz_excpetion import BizException
from utils.date_util import DateUtil
from utils.hive_utils import HiveUtil


class DateCheck(object):
    def __init__(self, data_date, date_range, db_name, table_name):
        self.data_date = data_date  # 数据日期
        self.date_range = date_range  # 分区范围(年/月/季/不分区)
        self.db_name = db_name
        self.table_name = table_name

    @property
    def data_scope(self):
        return self.data_scope

    @data_scope.setter
    def data_scope(self, date_scope):
        self.data_scope = date_scope

    @property
    def start_date(self):
        return self.start_date

    @start_date.setter
    def start_date(self,start_date):
        self.start_date = start_date

    @property
    def end_date(self):
        return self.end_date

    @end_date.setter
    def end_date(self,end_date):
        self.end_date = end_date

    def set_range(self):
        if self.date_range == DatePartitionRange.MONTH:
            self.data_scope = self.data_date[0:6]
            self.start_date = DateUtil().get_month_start(self.data_date)
            self.end_date = DateUtil().get_month_end(self.data_date)

        elif self.date_range == DatePartitionRange.QUARTER_YEAR:
            self.data_scope = DateUtil().get_quarter(self.data_date)
            self.start_date = DateUtil().get_month_start(self.data_date)
            self.end_date = DateUtil().get_month_end(self.data_date)
        elif self.date_range == DatePartitionRange.YEAR:
            self.data_scope = self.data_date[0:4]
            self.start_date = DateUtil().get_year_start(self.data_date)
            self.end_date = DateUtil().get_year_end(self.data_date)


    def date_partition_check(self):
        """
            判断待归档的数据对象的日期分区范围是否与目标表一致
        :return:
        """
        # 先判断Hive中是否已经存在该表
        if HiveUtil.exist_table(self.db_name, self.table_name):
            if self.date_range == DatePartitionRange.ALL_IN_ONE \
                    and HiveUtil.has_partition(self.db_name, self.table_name):
                raise BizException("归档日期分区与Hive表不一致 ！！！")

        # 获取日期分区范围,并获取开始和结束日期
        self.set_range()

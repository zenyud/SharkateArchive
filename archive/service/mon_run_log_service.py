# -*- coding: UTF-8 -*-  

# Date Time     : 2019/1/8
# Write By      : adtec(ZENGYU)
# Function Desc : 执行日志记录
# History       : 2019/1/8  ZENGYU     Create
# Remarks       :
from archive.dao.mon_run_log_dao import MonRunLogDao


class MonRunLogService(object):
    mon_run_log_dao = MonRunLogDao()

    def create_run_log(self, didp_mon_run_log):
        """
            新增运行执行日志
        :param didp_mon_run_log 执行日志对象:
        :return:
        """
        self.mon_run_log_dao.add_mon_run_log(didp_mon_run_log)

    def find_run_logs(self, table_name, obj, start_date, end_date):
        self.mon_run_log_dao.get_mon_run_log_list(table_name, obj, "5",
                                                  start_date, end_date)

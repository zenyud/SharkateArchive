# -*- coding: UTF-8 -*-  

# Date Time     : 2019/1/8
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2019/1/8  ZENGYU     Create
# Remarks       :
from archive.model.base_model import DidpMonRunLog
from utils.base_connect import get_session

SESSION = get_session()


class MonRunLogDao(object):
    @staticmethod
    def add_mon_run_log(didp_mon_run_log):
        """
            新增执行日记记录
        :param didp_mon_run_log: 执行日志对象
        :return:
        """
        SESSION.add(didp_mon_run_log)
        SESSION.commit()
        SESSION.close()

    def get_mon_run_log(self, pro_id, biz_date, org, batch_no):
        """
            获取执行日志
        :param pro_id: 执行号
        :param biz_date: 业务日期
        :param org: 机构
        :param batch_no: 批次号
        :return:
        """
        pass

    @staticmethod
    def get_mon_run_log_list(table_name, obj, pros_type, start_date, end_date):
        """
            获取执行日志集合
        :param table_name:  目标表
        :param obj: 数据对象
        :param pros_type: 加工类型
        :param start_date: 执行日期
        :param end_date: 结束
        :return:
        """
        result = SESSION.query(DidpMonRunLog).filter(
            DidpMonRunLog.TABLE_NAME == table_name,
            DidpMonRunLog.DATA_OBJECT_NAME == obj,
            DidpMonRunLog.PROCESS_TYPE == pros_type,
            DidpMonRunLog.BIZ_DATE >= start_date,
            DidpMonRunLog.BIZ_DATE <= end_date,
            DidpMonRunLog.PROCESS_STATUS == "1",  # 执行状态为成功
            DidpMonRunLog.ERR_MESSAGE == ""  # 没有报错信息
        ).all()
        if len(result) < 1:
            return None
        else:
            return result

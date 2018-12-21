# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 参数初始化
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from archive.model.data_mode_source import DataModeSource
from data_file_archive_constance import DataFileArchiveConstance
from utils.date_util import DateUtil
from utils.str_utils import StringUtil
from utils.biz_excpetion import BizExcption


def init_check(paras):
    """
    :param paras: Map<String,String> 参数字典
    :return:

    """
    log_user = paras.get(DataFileArchiveConstance.LOGIN_USER)

    sys_name = get_arg(paras, DataFileArchiveConstance.SYS_NAME )
    obj_name = get_arg(paras,DataFileArchiveConstance.OBJ_NAME )
    obj_id = str(paras.get(DataFileArchiveConstance.OBJ_ID))
    table_name = get_arg(paras, DataFileArchiveConstance.TABLE_NAME)
    org = get_arg(paras, DataFileArchiveConstance.ORG)
    data_date = get_arg(paras, DataFileArchiveConstance.DATA_DATE)
    # 获取前一天的时间
    yes_date = DateUtil.get_day_of_day(data_date,-1)
    hive_db = str(paras.get(DataFileArchiveConstance.HIVE_DB))
    if StringUtil.is_blank(hive_db):
        hive_db = sys_name

    dataModeSource = paras.get(DataFileArchiveConstance.DATA_MODE_SOURCE)





def get_arg(paras, key):
    """
    :param paras: 参数Map
    :param key: 参数名
    :return: 参数值
    """
    value =str(paras.get(key))
    if StringUtil.is_blank(value):
        raise BizExcption("缺少参数{0}".format(key))
    return value.strip().upper()



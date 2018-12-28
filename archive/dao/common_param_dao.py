# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/26
# Write By      : adtec(ZENGYU)
# Function Desc : 公共代码模块
# History       : 2018/12/26  ZENGYU     Create
# Remarks       :
from archive.model.base_model import DidpCommonParams
from utils.base_connect import get_session

SESSION = get_session()


class CommonParamsDao():
    """
        操作公共代码类
    """

    def get_common_param(self, group_name, param_name):
        """
        获取公共参数
        :param group_name: 组名
        :param param_name: 参数名
        :return:
        """
        result = SESSION.query(DidpCommonParams).filter(
            DidpCommonParams.GROUP_NAME == group_name,
            DidpCommonParams.PARAM_NAME == param_name).one()
        return result

    def get_all_common_code(self):

        """
            获取所有的公共参数放于dict中
        :return:
        """
        result = SESSION.query(DidpCommonParams).all()
        common_dict = {}
        for r in result:
            common_dict[r.PARAM_NAME] = r.PARAM_VALUE

        return common_dict

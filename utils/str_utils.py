# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 字符串工具类
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :


class StringUtil(object):
    @classmethod
    def is_blank(self, in_str):
        """
        判断字符串是否为空
        :param input: 输入的字符串
        :return: true or false
        """
        if in_str is None :
            return True
        elif in_str.strip().__len__()==0 :
            return True
        else:
            return False



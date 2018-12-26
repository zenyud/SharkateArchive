# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/25
# Write By      : adtec(ZENGYU)
# Function Desc : 接入表字段信息
# History       : 2018/12/25  ZENGYU     Create
# Remarks       :


class SourceFieldInfo(object):
    def __init__(self, col_name, data_type, default_value, not_null, unique,
                 comment):
        self.col_name = col_name
        self.data_type = data_type
        self.default_value = default_value
        self.not_null = 0 if not_null == 'No' else 1
        self.unique = unique
        self.comment = comment

    @property
    def col_length(self):
        """
            获取字段的长度
        :return: 字段长度 int

        """
        if self.data_type.__contains__("(") and self.data_type.__contains__(
                     ")"):
                 index = self.data_type.index("(")
                 return int(self.data_type[index+1:-1])
        else:
            return 0
        
    @property
    def col_scale(self):
        """
            获取小数位数
        :return: 小数位数
        """
        if self.data_type.upper().__eq__("DECIMAL"):
            return 2
        return 0
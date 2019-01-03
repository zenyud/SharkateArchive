# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/25
# Write By      : adtec(ZENGYU)
# Function Desc : 接入表字段信息
# History       : 2018/12/25  ZENGYU     Create
# Remarks       :


class HiveFieldInfo(object):
    """
        Hive 表信息对象
    """

    def __init__(self, col_name, data_type, default_value, not_null, unique,
                 comment, col_seq):

        self.col_name = col_name
        if "(" not in data_type:
            self.data_type = data_type
        else:
            index = data_type.index("(")
            self.data_type = data_type[:index]
        self.default_value = default_value
        self.not_null = 0 if not_null == 'No' else 1
        self.unique = unique
        self.comment = comment
        self.col_seq = col_seq    # 字段序号

    @property
    def col_length(self):
        """
            获取字段的长度
        :return: 字段长度 int
        """
        if "(" in self.data_type and ")" in self.data_type:
            index1 = self.data_type.index("(")
            index2 = self.data_type.index(")")
            s = self.data_type[index1 + 1, index2]
            list = s.split(",")
            return int(list[0])
        else:
            return None

    @property
    def col_scale(self):
        """
            获取小数位数
        :return: 小数位数
        """
        if "(" in self.data_type and ")" in self.data_type:
            index1 = self.data_type.index("(")
            index2 = self.data_type.index(")")
            s = self.data_type[index1 + 1, index2]
            list = s.split(",")
            if len(list) == 2:
                return int(list[1])
        else:
            return None

    @property
    def col_name_quote(self):
        return "`" + self.col_name + "`"


class MetaTypeInfo(object):
    def __init__(self, field_type, field_length, field_scale):
        """
        :param field_type: 字段类型
        :param field_length: 字段长度
        :param field_scale: 字段精度
        """
        self.field_type = field_type
        self.field_length = field_length
        self.filed_scale = field_scale

    def __eq__(self, obj):
        """
            重写__eq__方法
        :param obj:
        :return:
        """
        if type(obj) == type(self):
            if obj.field_length == self.field_length and obj.field_type.__eq__(
                    self.field_type) and obj.filed_scale == self.filed_scale:
                return True
            else:
                return False
        elif obj is None:
            return False
        else:
            super(MetaTypeInfo, self).__eq__(obj)

    def get_whole_type(self):
        types = ["DECIMAL", "DOUBLE", "FLOAT"]
        if self.field_length > 0:
            if self.filed_scale > 0:
                return self.field_type + "(" + self.field_length + "," + self.filed_scale + ")"
            else:
                if self.field_type in types:
                    return self.field_type + "(" + self.field_length + "," + self.filed_scale + ")"
                else:
                    return self.field_type + "(" + self.field_length + ")"

        else:
            return self.field_type

    def set_whole_type(self, whole_type):
        """

            解析字段 获取字段长度和精度
        :param whole_type:  如 DECIMAL(M,N)
        :return:
        """
        if "(" in whole_type and ")" in whole_type:
            index1 = whole_type.index("(")
            index2 = whole_type.index(")")
            s = whole_type[index1 + 1, index2]
            self.field_type = whole_type[0:index1]
            list = s.split(",")
            if len(list) == 1:
                self.field_length = int(list[0])
            elif len(list) == 2:
                self.field_length, self.filed_scale = [int(x) for x in list]
        else:
            self.field_type = whole_type

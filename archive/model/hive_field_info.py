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

        self.full_type = data_type
        self.col_name = col_name
        if "(" not in data_type:
            self.data_type = data_type

        else:
            index1 = data_type.index("(")
            self.data_type = data_type[:index1]
        self.default_value = default_value
        self.not_null = 0 if not_null == 'No' else 1
        self.unique = unique
        self.comment = comment
        self.col_seq = col_seq  # 字段序号

    def get_list(self):
        if "(" in self.full_type and ")" in self.full_type:
            index1 = self.full_type.index("(")
            index2 = self.full_type.index(")")
            list = self.full_type[index1 + 1:index2].split(",")
            return list
        else:
            return None

    @property
    def col_length(self):
        list = self.get_list()
        if list:
            return list[0]
        else:
            return None
            # return self.col_length

    @col_length.setter
    def col_length(self, v):
        self.col_length = v

    @property
    def col_scale(self):
        list = self.get_list()
        if list:
            if len(list) == 2:
                return list[1]
        else:
            return None

    @col_scale.setter
    def col_scale(self, v):
        self.col_scale = v

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

    @property
    def get_whole_type(self):
        types = ["DECIMAL", "DOUBLE", "FLOAT"]
        if self.field_length > 0:
            if self.filed_scale > 0:
                return str(
                    self.field_type + "(" + self.field_length + "," + self.filed_scale + ")")
            else:
                if self.field_type in types:
                    return str(
                        self.field_type + "(" + self.field_length + "," + self.filed_scale + ")")
                else:
                    return str(self.field_type + "(" + self.field_length + ")")

        else:
            return str(self.field_type)

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


if __name__ == '__main__':
    a = HiveFieldInfo("a", "varchar(30)", "", "", "", "", 1)
    a.col_scale = 1
    print a.full_type
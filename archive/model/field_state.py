# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/28
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/28  ZENGYU     Create
# Remarks       :
from archive.model.hive_field_info import MetaTypeInfo


class FieldState(object):
    def __init__(self, field, full_seq, current_seq, ddl_type, hive_type,
                 comment_hive, comment_ddl, hive_no):
        # type: (object, object, object, MetaTypeInfo, MetaTypeInfo, object, object, object) -> FieldState
        self.field = field
        self.full_seq = full_seq
        self.current_seq = current_seq
        self.ddl_type = ddl_type
        self.hive_type = hive_type
        self.comment_hive = comment_hive
        self.comment_ddl = comment_ddl
        self.hive_no = hive_no  # -1:hive去掉字段; -2:ddl增加字段; 其余正常已有hive序号






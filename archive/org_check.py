# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 机构检查
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from utils.biz_excpetion import BizException
from utils.hive_utils import HiveUtil


class OrgCheck(object):
    def __init__(self, db_name, table_name, org_pos):
        self.db_name = db_name
        self.table_name = table_name
        self.org_pos = org_pos

    def check(self):
        if (HiveUtil.exist_table(self.db_name, self.table_name)):
            if self.org_pos != HiveUtil.get_org_pos(self.db_name,
                                                      self.table_name):
                raise BizException("归档机构分区与hive表中不一致 !!!")

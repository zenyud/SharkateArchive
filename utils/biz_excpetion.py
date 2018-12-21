# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 自定义异常类
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :

class BizExcption(Exception):
    def __init__(self,*args):
        self.args = args
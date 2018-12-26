# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/26
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/26  ZENGYU     Create
# Remarks       :
from uuid import uuid1

def get_uuid():
    return str(uuid1()).replace("-","")

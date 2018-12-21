# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 日期工具类
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from datetime import date,timedelta

import datetime

class  DateUtil(object):

    @classmethod
    def get_day_of_day(cls, dayof, n=0):
        '''''
        if n>=0,date is larger than today
        if n<0,date is less than today
        date format = "YYYYMMDD"
        '''
        a = datetime.datetime.strptime(dayof, "%Y%m%d")
        result = None
        if (n < 0):
            n = abs(n)
            result =  a - timedelta(n)
        else:
            result =  a + timedelta(n)
        return result.strftime("%Y%m%d")

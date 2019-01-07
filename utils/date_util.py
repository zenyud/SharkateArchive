# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc : 日期工具类
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from datetime import date, timedelta
import datetime
import calendar


class DateUtil(object):

    @classmethod
    def get_day_of_day(cls, dayof, n=0):
        '''''
        获取给顶日期的 前N 天或后N 天数据
        if n>=0,date is larger than today
        if n<0,date is less than today
        date format = "YYYYMMDD"
        '''
        a = datetime.datetime.strptime(dayof, "%Y%m%d")
        result = None
        if (n < 0):
            n = abs(n)
            result = a - timedelta(n)
        else:
            result = a + timedelta(n)
        return result.strftime("%Y%m%d")

    @staticmethod
    def get_now_date():
        dt = datetime.datetime.now()
        return dt.strftime("%Y%m%d %H:%M:%S")


    @staticmethod
    def get_month_start(now_day):
        """
        :param now_day: 当日日期String
        :return: 月初日期String
        """
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month
        day_begin = '%d%02d01' % (year, month)

        return str(day_begin)

    @staticmethod
    def get_month_end(now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month
        monthRange = calendar.monthrange(year, month)[1]
        day_end = '%d%02d%02d' % (year, month, monthRange)
        return str(day_end)

    @staticmethod
    def get_year_start( now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        return str(year) + "0101"

    @staticmethod
    def get_year_end(now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        return str(year) + "1231"
        pass

    @staticmethod
    def get_quarter_start( now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month
        if month > 0 and month < 4:
            jd_begin = str(year) + '0101'

        elif month > 3 and month < 7:
            jd_begin = str(year) + '0401'

        elif month > 6 and month < 10:
            jd_begin = str(year) + '0701'

        else:
            jd_begin = str(year) + '1001'
        return jd_begin

    @staticmethod
    def get_quarter_end(now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month

        if month > 0 and month < 4:
            jd_end = str(year) + '0331'
        elif month > 3 and month < 7:
            jd_end = str(year) + '0731'
        elif month > 6 and month < 10:
            jd_end = str(year) + '0931'
        else:
            jd_end = str(year) + '1231'
        return jd_end

    @staticmethod
    def get_quarter(now_day):
        d = datetime.datetime.strptime(now_day, "%Y%m%d")
        year = d.year
        month = d.month
        if month >= 1 and month <= 3:
            quarter = str(year) + "Q1"
        elif month >= 4 and month <= 6:
            quarter = str(year) + "Q2"
        elif month >= 7 and month <= 9:
            quarter = str(year) + "Q3"
        else:
            quarter = str(year) + "Q4"
        return quarter

if __name__ == '__main__':
   print DateUtil().get_day_of_day("20190107",-1)

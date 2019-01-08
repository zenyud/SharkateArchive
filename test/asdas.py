# -*- coding: UTF-8 -*-  

# Date Time     : 2019/1/8
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2019/1/8  ZENGYU     Create
# Remarks       :
from abc import ABCMeta

import abc


class A(object):
    __metaclass__ = ABCMeta

    @abc.abstractmethod
    def ask(self):
        print 1


class B(A):
    def ask(self):
        super(B, self).ask()

if __name__ == '__main__':
    B().ask()
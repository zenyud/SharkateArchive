# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/21
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/21  ZENGYU     Create
# Remarks       :
from enum import Enum


class DataModeSource(Enum):
    ALL = "1"
    ADD = "2"
    def __init__(self,name, value ):
        self.name = name
        self.value = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value


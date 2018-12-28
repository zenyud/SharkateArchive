# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/28
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/28  ZENGYU     Create
# Remarks       :

class Student(object):
    def __init__(self,v):
        self.v = v
        pass

    @property
    def score(self):
        return self._score

    @score.setter
    def score(self, value):
        if not isinstance(value, int):
            raise ValueError('score must be an integer!')
        if value < 0 or value > 100:
            raise ValueError('score must between 0 ~ 100!')
        self._score = value
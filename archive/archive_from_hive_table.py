# -*- coding: UTF-8 -*-
# Date Time     : 2019/1/2
# Write By      : adtec(ZENGYU)
# Function Desc :  归档执行主类
# History       : 2019/1/2  ZENGYU     Create
# Remarks       :
from archive.archive_way import *


def main():
    args = ArchiveData.archive_init()
    # 判断归档模式 执行不同的实现类

    switch = {"1": AllArchive(), "2": AddArchive(), "4": ChainTransArchive(),
              "5": LastAddArchive(), "6": LastAllArchive()}
    print args.saveMode
    archive = switch.get(args.saveMode)
    archive.run()

if __name__ == '__main__':
    main()
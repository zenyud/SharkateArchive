# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/26
# Write By      : adtec(ZENGYU)
# Function Desc : 获取配置文件解析器
# History       : 2018/12/26  ZENGYU     Create
# Remarks       :
import ConfigParser
import os


def get_root_path():
    """
    获取配置文件的绝对路径
    :return:
    """
    # 获取当前路径
    curPath = os.path.abspath(os.path.dirname(__file__))
    rootPath = curPath[:curPath.find("SharkateArchive\\") + len(
        "SharkateArchive\\")]  # SharkateArchive，也就是项目的根路径

    return rootPath


def get_conf_reader(conf_path, conf_name):
    """

    :param conf_path:
    :param conf_name:
    :return:
    """
    cf = ConfigParser.ConfigParser()
    cf.read(get_root_path() + "{path}\\{name}".format(path=conf_path,
                                                      name=conf_name))
    return cf

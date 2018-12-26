# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/24
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/24  ZENGYU     Create
# Remarks       :


from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import *

from archive.model.base_model import *

engine = create_engine("ibm_db_sa://db2inst1:111111@10.10.20.55:50000/skd")


if __name__ == '__main__':
    Session = sessionmaker(bind=engine)
    session = Session()
    ret = session.query(DidpHdsStructMetaCtrl).one()
    print ret


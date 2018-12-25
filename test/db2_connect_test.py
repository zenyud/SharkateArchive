# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/24
# Write By      : adtec(ZENGYU)
# Function Desc :
# History       : 2018/12/24  ZENGYU     Create
# Remarks       :


from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import *
import ibm_db_sa

from archive.model.base_model import HdsStructArchiveCtrl

engine = create_engine("ibm_db_sa://db2inst1:111111@10.10.20.55:50000/hds2")
Base = declarative_base()


class ArchiveCtrL(Base):
    __tablename__ = 'HDS_STRUCT_ARCHIVE_CTRL'
    # SYSTEM_NAME = Column(primary_key=True)
    OBJECT_NAME = Column(primary_key=True)
    STATUS = Column()
    CREATE_USER = Column()
    CREATE_TIME = Column()
    ORG_CODE = Column(primary_key=True)
    STORAGE_MODE = Column(primary_key=True)

    pass

if __name__ == '__main__':
    Session = sessionmaker(bind=engine)
    session = Session()
    ret = session.query(HdsStructArchiveCtrl).filter(HdsStructArchiveCtrl.OBJECT_NAME == 'A',
                       HdsStructArchiveCtrl.ORG_CODE == '1',
                       HdsStructArchiveCtrl.STORAGE_MODE == '1').all()
    print len(ret)


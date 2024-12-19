from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pymysql
import os

db_host = os.environ.get('MYSQL_HOST', '')
db_port = os.environ.get('MYSQL_PORT', '')
db_user = os.environ.get('MYSQL_USER', '')
db_password = os.environ.get('MYSQL_PASSWORD', '')
db_name = os.environ.get('MYSQL_DB', '')

class Engine_Creater:
    def __init__(self):
        self.engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}", echo=True)
        self.session = Session(self.engine)

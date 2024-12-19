from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from snowflake.sqlalchemy import URL
import os
from dotenv import load_dotenv

load_dotenv()
account_identifier = os.environ.get('ACCOUNT_IDENTIFIER', '')
print('account_identifier: ', account_identifier)
account_user = os.environ.get('ACCOUNT_USER', '')
print('account_user: ', account_user)
account_password = os.environ.get('ACCOUNT_PASSWORD', '')
print('account_password: ', account_password)
database_name = os.environ.get('DATABASE_NAME', 'PROFILER')
print('database_name: ', database_name)
database_schema = os.environ.get('DATABASE_SCHEMA', 'PUBLIC')

class Engine_Creater:
    def __init__(self) -> None:
        self.engine = create_engine(
                URL(
                    account=account_identifier,
                    user=account_user,
                    password=account_password,
                    database=database_name,
                    schema=database_schema
                ),
                echo=True
            )
        
        self.session = Session(self.engine)
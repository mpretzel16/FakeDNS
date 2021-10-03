import os

import sqlalchemy.databases
import sqlalchemy.exc
import toml
from sqlalchemy import *
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects import oracle
from sqlalchemy.orm import sessionmaker
from .sql_alchemy_base import Base
from urllib.parse import quote

class Database:
    metadata = MetaData()
    Session: sessionmaker
    engine: sqlalchemy.engine
    bool_connected = False
    bool_sqlite = False
    database_config: dict
    def __init__(self, database_config: dict):
        try:
            self.database_config = database_config
            database_url = self.create_database_url()
            if not database_url['bool_error']:
                # print(database_url)
                conn = self.connect(database_url['database_url'])
                # print(conn)
                if conn['bool_connected']:
                    self.bool_connected = True
                Base.metadata.create_all(self.engine)
                self.Session = sessionmaker(bind=self.engine)
            else:
                print(database_url['str_error'])
        except Exception as e:
            print(e)

    def create_database_url(self):
        dict_return = {'bool_error': False, 'str_error': '', 'database_url': None}
        try:
            database_configuration = self.database_config
            if database_configuration['database_type'] == 'postgresql':
                passwd = quote(database_configuration['database_password'])
                db_api = ""
                if database_configuration['database_dbapi'] == "psycopg2" or\
                        database_configuration['database_dbapi'] == "pg8000":
                    db_api = "+{}".format(database_configuration['database_dbapi'])
                database_url = 'postgresql{}://{}:{}@{}/{}'.format(db_api,database_configuration['database_user'],
                                                                   passwd,
                                                                   database_configuration['database_host'],
                                                                   database_configuration['database_name'])
                dict_return['database_url'] = database_url
        except Exception as e:
            dict_return['bool_error'] = True
            dict_return['str_error'] = str(e)
        return dict_return

    def connect(self, database_url):
        dict_return = {'bool_error': False, 'str_error': '', 'bool_connected': False}
        try:
            self.engine = create_engine(database_url)
            if self.engine.connect():
                dict_return['bool_connected'] = True
        except sqlalchemy.exc.OperationalError as e:
            dict_return['bool_error'] = False
            dict_return['str_error'] = str(e)
        except Exception as e:
            dict_return['bool_error'] = False
            dict_return['str_error'] = str(e)
        return dict_return

    class IPv4_Networks(Base):
        __tablename__ = 'IPv4_Networks'
        __table_args__ = {'extend_existing': True}
        network = Column(String(20), primary_key=True)
        country_id = Column(Integer(), nullable=False)

    class IPv6_Networks(Base):
        __tablename__ = 'IPv6_Networks'
        __table_args__ = {'extend_existing': True}
        network = Column(String(43), primary_key=True)
        country_id = Column(Integer(), nullable=False)

    class Country_List(Base):
        __tablename__ = 'country_list'
        __table_args__ = {'extend_existing': True}
        country_id = Column(Integer(),primary_key=True, nullable=False)
        country_code = Column(String(5), nullable=False)
        country_name = Column(String(255), nullable=False)
        ccTLD = Column(String(4), nullable=True)

    class Dns_Authoritative(Base):
        __tablename__ = 'dns_authoritative'
        __table_args__ = {'extend_existing': True}
        entry_text = Column(String(255),primary_key=True, nullable=False)
        bool_authoritative = Column(Boolean(), nullable=False)
        authoritative_data = Column(Text(), nullable=True)

    class Dns_Entries(Base):
        __tablename__ = 'dns_entries'
        __table_args__ = {'extend_existing': True}
        id = Column(Integer(), primary_key=True,autoincrement = True)
        entry_text = Column(String(255),primary_key=False, nullable=False)
        record_type = Column(Integer(), nullable=False)
        record_data = Column(JSON(), nullable=False)

    class Tld_Listing(Base):
        __tablename__ = 'tld_listing'
        __table_args__ = {'extend_existing': True}
        domain = Column(String(20),primary_key=True, nullable=False)
        type = Column(String(20), nullable=False)
        org = Column(String(255), nullable=True)
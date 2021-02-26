import pandas as pd
from typing import List, Tuple
import urllib
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.query import Query

class DatabaseCredentials:
    def __init__(self, uid: str, pwd: str) -> None:
        self.uid = uid
        self.pwd = pwd

class Database:
    def __init__(self, server: str, database: str, credentials: DatabaseCredentials, fast_executemany: bool=True) -> None:
        self.server = server
        self.database = database
        self.reset_metadata()
        self.tables = None
        self.engine = self.get_engine(credentials, fast_executemany)
        self.session = sessionmaker(self.engine)()

    def get_engine(self, credentials: DatabaseCredentials, fast_executemany: bool=True) -> Engine:
        driver = "{ODBC Driver 17 for SQL Server}"
        connection_params = urllib.parse.quote_plus(f"DRIVER={driver};SERVER=tcp:{self.server},1433;DATABASE={self.database};UID={credentials.uid};PWD={credentials.pwd}")
        connection_string = f"mssql+pyodbc:///?odbc_connect={connection_params}"
        return create_engine(connection_string, fast_executemany=fast_executemany)

    def table(self, table_name: str, schema: str, primary_key_columns: List[str]):
        Table(
            table_name,
            self.metadata,
            *[Column(column, primary_key=True) for column in primary_key_columns],
            autoload=True,
            autoload_with=self.engine,
            schema=schema
        )

    def get_tables(self):
        Base = automap_base(metadata=self.metadata)
        Base.prepare()
        self.tables = Base.classes
        return self.tables

    def reset_metadata(self):
        self.metadata = MetaData()

    def read_to_df(self, query: Query) -> pd.DataFrame:
        return pd.read_sql(query.statement, self.engine)

    def write_to_sql(self, df: pd.DataFrame, table: str, schema: str, **kwargs):
        df.to_sql(
            name=table,
            con=self.engine,
            schema=schema,
            **kwargs
        )
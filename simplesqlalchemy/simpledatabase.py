from typing import Any, Callable, List, Tuple, Union
import urllib

import pandas as pd
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.automap import AutomapBase, automap_base, classname_for_table
from sqlalchemy.orm.query import Query
import pyodbc

from simplesqlalchemy.schema_collection import SchemaCollection

class Credentials:
    def __init__(self, uid: str, pwd: str) -> None:
        self.uid = uid
        self.pwd = pwd


class Database:
    def __init__(self, server: str, database: str, credentials: Credentials) -> None:
        self.server: str = server
        self.database: str = database
        self.credentials = credentials
        self.reset_metadata()
        self.tables: SchemaCollection = None
        self.reset_engine()
        self.sessionm = self.create_session()

    @property
    def t(self) -> SchemaCollection:
        return self.tables

    def reset_session(self) -> None:
        self.session = self.create_session()

    def create_session(self) -> Session:
        return sessionmaker(self.engine)()

    def reset_engine(self) -> None:
        self.engine = self.create_engine()

    def create_engine(self) -> Engine:
        return create_engine(
            self.connection_string(
                credentials=self.credentials
            ),
            fast_executemany=True,
            pool_pre_ping=True
        )

    def reset_metadata(self):
        self.metadata = MetaData()

    def connection_string(self, credentials: Credentials) -> str:
        driver = "{ODBC Driver 17 for SQL Server}"
        connection_params = urllib.parse.quote_plus(
            f"DRIVER={driver};SERVER=tcp:{self.server},1433;DATABASE={self.database};UID={credentials.uid};PWD={credentials.pwd}")
        return f"mssql+pyodbc:///?odbc_connect={connection_params}"

    def table(self, table_name: str, schema: str, columns: List[Column] = [], primary_key_columns: List[str] = []):
        return Table(
            table_name,
            self.metadata,
            *columns,
            *[Column(column, primary_key=True)
              for column in primary_key_columns],
            autoload=True,
            autoload_with=self.engine,
            schema=schema
        )

    def prepare_tables(self):
        base = automap_base(metadata=self.metadata)
        base.prepare(
            classname_for_table=self._schema_qualified_classname_for_table
        )
        self.classes_ = base.classes
        self.tables = SchemaCollection(base.classes)
        return self.tables

    def query(self, *args, **kwargs) -> Query:
        return self.session.query(*args, **kwargs)

    def to_df(self, query: Query) -> pd.DataFrame:
        return pd.read_sql(query.statement, self.engine)

    def write_df(self, df: pd.DataFrame, table: Union[Table, str], **kwargs):
        if isinstance(table, str):
            df.to_sql(
                con=self.engine,
                name=table,
                **kwargs
            )
        else:
            if not isinstance(table, Table):
                try:
                    table = table.__table__
                except AttributeError:
                    raise ValueError(
                        f"table of type {table.__class__} is not supported")
            df.to_sql(
                con=self.engine,
                name=table.name,
                schema=table.schema,
                **kwargs
            )

    def insert(self, rows):
        if isinstance(rows, list):
            self.session.add_all(rows)
        else:
            self.session.add(rows)

    def commit(self):
        self.session.commit()

    def _schema_qualified_classname_for_table(self, base: AutomapBase, tablename: str, table: Table) -> str:
        return f"__{table.schema}__{tablename}"

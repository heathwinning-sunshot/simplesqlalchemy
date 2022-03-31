from __future__ import annotations
from typing import Any, Dict, Tuple
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.sql.schema import Column

class TableSugar:
    """
    Syntactic sugar over DeclarativeMeta to allow columns to be easily accessed through string subscripts
    """
    table_: DeclarativeMeta

    def __init__(self, table: DeclarativeMeta) -> None:
        self.table_ = table

    def __getitem__(self, name: str) -> Column:
        """
        Allow columns to be accessed through subscripts
        """
        return self.table_.__table__.columns[name]

    def __getattr__(self, name: str) -> Any:
        """
        Pass all attribute calls to `self.table_`
        """
        return self.table_.__getattribute__(self.table_, name)

    def __repr__(self) -> str:
        return repr(self.table_)

    def __str__(self) -> str:
        return str(self.table_)
class TableCollection:
    tables_: Dict[str, TableSugar]

    def __init__(self) -> None:
        self.tables_ = dict()

    def __getitem__(self, name: str) -> TableSugar:
        return self.__getattr__(name)

    def __getattr__(self, name: str) -> TableSugar:
        if name in self.tables_:
            return self.tables_[name]
        else:
            raise KeyError(f"no table named {name}")

    def add_table(self, tablename: str, table: DeclarativeMeta):
        self.tables_[tablename] = TableSugar(table)

    def __repr__(self) -> str:
        return repr(self.tables_)

    def __str__(self) -> str:
        return str(self.tables_)

class SchemaCollection:
    schemas_: Dict[str, TableCollection]

    def __init__(self, classes) -> None:
        self.schemas_ = dict()
        self.populate_from_classes(classes)

    def __getitem__(self, name: str) -> TableCollection:
        return self.__getattr__(name)

    def __getattr__(self, name: str) -> TableCollection:
        if name in self.schemas_:
            return self.schemas_[name]
        else:
            raise AttributeError(f"no schema named {name}")
    
    def add_table_to_schema(self, schema: str, tablename: str, table: DeclarativeMeta):
        if schema not in self.schemas_:
            self.schemas_[schema] = TableCollection()

        self.schemas_[schema].add_table(tablename, table)
    
    def populate_from_classes(self, classes):
        for table in classes:
            schema, tablename = table_from_schema_qualified_classname(table.__name__)
            self.add_table_to_schema(schema, tablename, table)

    def __repr__(self) -> str:
        return repr(self.schemas_)
    
    def __str__(self) -> str:
        return str(self.schemas_)

def table_from_schema_qualified_classname(schema_qualified_classname: str) -> Tuple[str, str]:
    _, schema, tablename = schema_qualified_classname.split("__")
    return (schema, tablename)
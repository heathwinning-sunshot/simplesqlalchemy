from __future__ import annotations
from typing import Any, Dict, Tuple
from sqlalchemy.orm.decl_api import DeclarativeMeta

class TableCollection:
    tables_: Dict[str, DeclarativeMeta]

    def __init__(self) -> None:
        self.tables_ = dict()

    def __getattr__(self, name: str) -> DeclarativeMeta:
        if name in self.tables_:
            return self.tables_[name]
        else:
            raise KeyError(f"no table named {name}")

    def add_table(self, tablename: str, table: DeclarativeMeta):
        self.tables_[tablename] = table

    def __repr__(self) -> str:
        return repr(self.tables_)

    def __str__(self) -> str:
        return str(self.tables_)

class SchemaCollection:
    schemas_: Dict[str, TableCollection]

    def __init__(self, classes) -> None:
        self.schemas_ = dict()
        self.populate_from_classes(classes)

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
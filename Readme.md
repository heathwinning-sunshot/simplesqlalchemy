# simplesqlalchemy
A small wrapper around sqlalchemy to reduce boilerplate when using sqlalchemy and pandas.

## Installation
`pipenv install git+https://github.com/heathwinning-sunshot/simplesqlalchemy#egg=simplesqlalchemy`

## Usage
```python
from datetime import datetime
from dateutil.relativedelta import relativedelta
from simplesqlalchemy import Database, Credentials
from sqlalchemy.sql import and_

db_creds = Credentials(
    uid="uid",
    pwd="pwd"
)
db = Database(
    server="server_url",
    database="database_name",
    credentials=db_creds
)

# define tables to be used
some_table = db.table(
    table_name="Some_Table",
    schema="schema",
    primary_key_columns=["column_1", "column_2"]
)
db.prepare_tables()

# read query from the database to a dataframe
df = db.to_df(
    db.query(
        some_table.column_1,
        some_table.column_2,
        some_table.modified_date
    ).filter(
        and_(
            some_table.modified_date <= datetime.today()+relativedelta(years=-1),
            some_table.column_2 >= some_table.column_1
        )
    )
)

# modify dataframe
df["column_1"] = df["column_2"] * 2
df["modified_date"] = datetime.today()

# write dataframe back to table
db.write_df(df, table=some_table, if_exists="append")
```
from typing import List

import sqlalchemy as db

from efa_30mhz.sync import Source


class SQLSource(Source):
    def __init__(self, conn_string, table_name, **kwargs):
        super(SQLSource, self).__init__(**kwargs)
        self.conn_string = conn_string
        self.table_name = table_name
        self.engine = db.create_engine(conn_string)
        self.connection = None

    def __enter__(self):
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def read_all(self) -> List:
        sample_data = db.Table(self.table_name, self.metadata, autoload=True, autoload_with=self.engine)
        query = db.select([sample_data])
        result = self.connection.execute(query).fetchall()
        return result

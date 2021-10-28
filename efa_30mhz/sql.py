from typing import List

import sqlalchemy as db

from efa_30mhz.sync import Source


class SQLSource(Source):
    @staticmethod
    def to_thirty_mhz(**kwargs):
        pass

    def __init__(self, conn_string, table_name, **kwargs):
        super(SQLSource, self).__init__(**kwargs)
        self.metadata = db.MetaData()
        self.conn_string = conn_string
        self.table_name = table_name
        self.engine = db.create_engine(conn_string)
        self.connection = None

    def read_all(self) -> List:
        self.connection = self.connection or self.engine.connect()
        sample_data = db.Table(
            self.table_name, self.metadata, autoload=True, autoload_with=self.engine
        )
        query = db.select([sample_data])
        result = self.connection.execute(query).fetchall()
        self.connection.close()
        return result

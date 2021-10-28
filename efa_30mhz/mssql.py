from typing import List
import pymssql
import pandas

from efa_30mhz.sync import Source


class MSSQLSource(Source):
    def __init__(
            self, server, user, password, database, port, table=None, query=None, **kwargs
    ):
        super().__init__(**kwargs)
        self.conn = pymssql.connect(
            server=server, user=user, password=password, database=database, port=port
        )
        self.table = table
        self.query = query

    @staticmethod
    def to_thirty_mhz(**kwargs):
        pass

    def read_all(self, *args, **kwargs) -> List:
        if not self.query and self.table:
            self.query = f"SELECT * FROM {self.table}"
        query = self.query
        if len(args) > 0:
            query = self.query.format(*args)
        df = pandas.read_sql(query, self.conn)
        result = df.to_dict(orient="records")
        return result

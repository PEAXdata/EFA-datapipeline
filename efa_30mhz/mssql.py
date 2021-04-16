from typing import List
import pymssql

from efa_30mhz.sync import Source


class MSSQLSource(Source):
    def __init__(self, server, user, password, database, port, table=None, query=None, **kwargs):
        super(MSSQLSource).__init__(**kwargs)
        self.conn = pymssql.connect(server=server, user=user, password=password,
                                    database=database, port=port)
        self.table = table
        self.query = query

    @staticmethod
    def to_thirty_mhz(**kwargs):
        pass

    def read_all(self) -> List:
        cursor = self.conn.cursor()
        if self.table:
            self.query = f'SELECT * FROM {self.table}'
        cursor.execute(self.query)
        result = list(cursor)
        return result

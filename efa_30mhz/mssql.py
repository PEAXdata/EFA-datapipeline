import urllib

from efa_30mhz.sql import SQLSource


class MSSQLSource(SQLSource):
    def __init__(self, conn_string, **kwargs):
        conn_string = urllib.parse.quote_plus(conn_string)
        conn_string = "mssql+pyodbc:///?odbc_connect={}".format(conn_string)
        super(MSSQLSource, self).__init__(conn_string=conn_string, **kwargs)

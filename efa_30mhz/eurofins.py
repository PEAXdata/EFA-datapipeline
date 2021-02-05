import json
import urllib
from typing import List, Dict
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


class MSSQLSource(SQLSource):
    def __init__(self, conn_string, **kwargs):
        conn_string = urllib.parse.quote_plus(conn_string)
        conn_string = "mssql+pyodbc:///?odbc_connect={}".format(conn_string)
        super(MSSQLSource, self).__init__(conn_string=conn_string, **kwargs)


class EurofinsSource(MSSQLSource):
    def __init__(self, **kwargs):
        super(EurofinsSource, self).__init__(**kwargs)

    def __enter__(self):
        return super(EurofinsSource, self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super(EurofinsSource, self).__exit__(exc_type, exc_val, exc_tb)

    def clean_data(self, row: Dict) -> List[Dict]:
        """
        Cleans a single row
        :param row: a dictionary row
        :return: row
        """
        column_mapping = {
            'orderSampleDataId': 'order_sample_data_id',
            'sampleId': 'sample_id',
            'sampleCode': 'sample_code',
            'creationDate': 'creation_date',
            'mainCategory': 'main_category',
            'subCategory': 'sub_category',
            'resultGroupData': 'result_group_data',
        }
        row = {
            column_mapping[k]: v for k, v in row.items()
            if k in column_mapping
        }

        row['result_group_data'] = json.loads(row['result_group_data'])

        rows = self.deduplicate_data(row)

        return rows

    def deduplicate_data(self, row: Dict) -> List[Dict]:
        column_mapping = {
            'resultValue': 'result_value',
            'resultValue2': 'result_value_2',
            'resultCode': 'result_code',
            'resultRank': 'result_rank',

        }
        for result_group in row['result_group_data']:
            for result_data in result_group['resultData']:
                new_row = row.copy()
                del new_row['result_group_data']
                for col_from, col_to in column_mapping.items():
                    if col_from in result_data:
                        new_row[col_to] = result_data[col_from]
                yield new_row

    def unique_id(self, item):
        return f"{item['order_sample_data_id']} - {item['sample_id']} - {item['result_code']}"

    def read_all(self):
        rows = super(EurofinsSource, self).read_all()
        rows = list(map(self.clean_data, rows))
        flat_rows = (
            item for sublist in rows for item in sublist
        )
        # Remove duplicates from data
        deduped_dicts = list({self.unique_id(item): item for item in flat_rows}.values())
        return deduped_dicts

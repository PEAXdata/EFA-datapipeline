import json
import pprint
from typing import List, Dict

from loguru import logger

from efa_30mhz.mssql import MSSQLSource


def unique_id(item: Dict) -> str:
    return f"{item['order_sample_data_id']} - {item['sample_id']} - {item['result_code']}"


def deduplicate_data(row: Dict) -> List[Dict]:
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


def to_standard_form(row: Dict) -> Dict:
    """
    Converts a single row to the standard format with an id, a name and a list of data
    :param row: a single Eurofins data row
    :return: A new dict
    """
    return {
        'id': row['sample_id'],
        'name': row['sample_code'],
        'data': [
            {
                'result_value': row.get('result_value', None),
                'result_value_2': row.get('result_value_2', None),
                'result_rank': row.get('result_rank', None),
                'result_code': row.get('result_code', None),
            }
        ]
    }


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

        rows = deduplicate_data(row)

        return rows

    def read_all(self):
        rows = super(EurofinsSource, self).read_all()
        rows = map(self.clean_data, rows)
        flat_rows = (
            item for sublist in rows for item in sublist
        )
        # Remove duplicates from data
        deduped_dicts = {unique_id(item): item for item in flat_rows}.values()
        standard_form = map(to_standard_form, deduped_dicts)
        return list(standard_form)

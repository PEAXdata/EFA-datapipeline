import json
from datetime import datetime
from typing import List, Dict, Iterable

import pytz
from loguru import logger

from efa_30mhz.sync import Source
from efa_30mhz.thirty_mhz import infer_type


class EurofinsSource(Source):
    def __init__(self, super_source: Source, already_done_in: str, package_codes: Dict[int, str],
                 metrics: Dict[str, str], **kwargs):
        super(EurofinsSource, self).__init__(**kwargs)
        self.already_done_in = already_done_in
        self.super_source = super_source
        self.package_codes = package_codes
        self.metrics = metrics

    def to_thirty_mhz(self, rows: List) -> (List, List, List, List):
        sensor_types = self.uniques(map(self.get_sensor_type, rows), id_column='id')
        import_checks = self.uniques(map(self.get_import_check, rows), id_column='id')
        ingests = list(map(self.get_ingests, rows))
        ids = list(map(lambda x: x['order_sample_data_id'], rows))
        return sensor_types, import_checks, ingests, ids

    def package_code_to_name(self, _id):
        return self.package_codes[int(_id)]

    def clean_single_result_data_point(self, result_data_point):
        column_mapping = {
            'resultDescription': 'result_description',
            'resultValue': 'result_value',
            'originCode': 'origin_code',
            'resultUnitOfMeasureDescription': 'result_unit_of_measure_description',
        }
        row = {
            column_mapping[k]: v for k, v in result_data_point.items()
            if k in column_mapping
        }
        return row

    def clean_result_group_data(self, result_group_data):
        final_result_group_data = []
        for result_group in result_group_data:
            for result_data_point in result_group['resultData']:
                final_result_group_data.append(self.clean_single_result_data_point(result_data_point))
        return final_result_group_data

    def infer_metric(self, unit_description, code):
        if code in self.metrics.keys():
            return self.metrics[code]
        return 'parsum'

    def get_sensor_type(self, row: Dict):
        # A sensor type is created per package code
        logger.debug(row)
        id = row['analysis_package_code']
        name = self.package_code_to_name(id)
        schema = {
            'file': {
                'name': 'File',
                'type': 'string',
            }
        }
        for result_data in row['result_group_data']:
            schema[result_data['origin_code']] = {
                'name': result_data['result_description'],
                'type': infer_type(result_data['result_value']),
                'metric': self.infer_metric(unit_description=result_data['result_unit_of_measure_description'],
                                            code=result_data['origin_code']),
            }
        return {'id': id, 'name': name, 'schema': schema}

    def uniques(self, data: Iterable[Dict], id_column) -> List[Dict]:
        done = []
        done_ids = set()
        for d in data:
            if d[id_column] not in done_ids:
                done_ids.add(d[id_column])
                done.append(d)
        return done

    def get_import_check(self, row: Dict):
        id = row['sample_id']
        name = row['sample_description']
        sensor_type = row['analysis_package_code']
        return {'id': id, 'name': name, 'sensor_type': sensor_type}

    def get_sample_file(self, row: Dict):
        return open('application.pdf', 'rb')

    def get_ingests(self, row: Dict):
        id = row['sample_id']
        data = {}
        for result_data in row['result_group_data']:
            data[result_data['origin_code']] = result_data['result_value']
        data['datetime'] = row['sample_date']
        data['file'] = self.get_sample_file(row)
        return {'id': id, 'data': [data]}

    def parse_timestamp(self, timestamp):
        return datetime.strptime(timestamp, '%Y-%m-%d').replace(tzinfo=pytz.timezone('Europe/Amsterdam'))

    def clean_data(self, row: Dict) -> Dict:
        """
        Cleans a single row
        :param row: a dictionary row
        :return: row
        """
        column_mapping = {
            'orderSampleDataId': 'order_sample_data_id',
            'sampleId': 'sample_id',
            'sampleCode': 'sample_code',
            'sampleDate': 'sample_date',
            'sampleDescription': 'sample_description',
            'analysisPackageCode': 'analysis_package_code',
            'creationDate': 'creation_date',
            'mainCategory': 'main_category',
            'subCategory': 'sub_category',
            'resultGroupData': 'result_group_data',
            'createdAt': 'created_at',
            'updatedAt': 'updated_at',
        }
        row = {
            column_mapping[k]: v for k, v in row.items()
            if k in column_mapping
        }
        row['result_group_data'] = self.clean_result_group_data(json.loads(row['result_group_data']))
        row['sample_date'] = self.parse_timestamp(row['sample_date'])
        return row

    def read_already_done(self, already_done_in):
        with open(already_done_in, 'r') as f:
            already_done = set(map(int, f.readlines()))
            return already_done

    def read_all(self):
        logger.info('Reading')
        already_done = self.read_already_done(self.already_done_in)
        rows = self.super_source.read_all()
        logger.debug(f'Found {len(rows)} rows')
        rows = list(filter(lambda x: len(x['result_group_data']) > 0 and x['order_sample_data_id'] not in already_done,
                           map(self.clean_data, rows)))
        logger.debug(f'Left with {len(rows)} rows after removing already done and rows without data.')
        return rows

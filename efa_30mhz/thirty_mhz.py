import json
from io import IOBase
from pprint import pformat
from typing import IO, Dict, Any

from loguru import logger
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone

import pytz
import requests

from efa_30mhz.sync import Target


class ThirtyMHzEndpoint(ABC):
    base_url = ""

    def __init__(self, tmz: 'ThirtyMHz'):
        self.tmz = tmz

    def list(self):
        return self.tmz.get(self.base_url)

    @abstractmethod
    def check(self, item, **kwargs):
        raise NotImplementedError

    def exists(self, **kwargs):
        l = self.list()
        for i in l:
            if self.check(i, **kwargs):
                return True
        return False

    def create(self, files=None, **kwargs):
        return self.tmz.post(self.base_url, self.get_data(**kwargs), files=files)

    @abstractmethod
    def get_data(self, item):
        raise NotImplementedError

    def get(self, **kwargs):
        l = self.list()
        for i in l:
            if self.check(i, **kwargs):
                return i
        return None


class ThirtyMHzError(Exception):
    def __init__(self, message):
        self.message = message
        super(ThirtyMHzError, self).__init__(message)


class SensorType(ThirtyMHzEndpoint):
    base_url = 'sensor-type'

    def check(self, item, **kwargs):
        return str(item['radioId']) == str(kwargs['id'])

    def get_data(self, id, name, data):
        json_keys, data_types = self.get_data_types(data)
        logger.debug(f'Data types: {data_types}. Json keys: {json_keys}')
        d = {
            'name': name,
            'description': name,
            'external': True,
            'jsonKeys': list(json_keys),
            'dataTypes': data_types,  # ['double'] * len(json_keys),
            'radioId': id
        }
        return d

    def get_data_types(self, data):
        types = {}
        for d in data:
            for col in d.keys():
                proposed_type = self.infer_type(d[col])
                # Type already exists
                if types.get(col, None) and types.get(col) != proposed_type:
                    raise ThirtyMHzError(f'Types for {col} don\'t correspond')
                else:
                    types[col] = proposed_type
        return list(types.keys()), list(types.values())

    def infer_type(self, col):
        print(col)
        if isinstance(col, float) or isinstance(col, int):
            return 'double'
        if isinstance(col, IOBase) or isinstance(col, str):
            return 'string'


class ImportCheck(ThirtyMHzEndpoint):
    base_url = 'import-check'

    def check(self, item, **kwargs):
        return item['sourceId'] == str(kwargs['id'])

    def get_data(self, id, name, sensor_type):
        json_keys = set()
        d = {
            'name': name,
            'description': name,
            'sensorType': sensor_type['typeId'],
            'enabled': True,
            'sourceId': id,
            'timezone': 'Europe/Amsterdam',
            'notificationRelevance': 300
        }
        return d

    def ingest(self, import_check, import_check_rows):
        data = []
        now = datetime.now(tz=pytz.timezone('Europe/Amsterdam'))
        now = now.replace(microsecond=0)
        now_str = now.isoformat()
        for row in import_check_rows:
            for r in row['data']:
                converted_r = self.convert_row(r)
                data.append({
                    'checkId': import_check['checkId'],
                    'data': converted_r,
                    'timestamp': now_str,
                    'status': 'ok'
                })

        logger.debug(f'Ingesting data: {pformat(data)}')
        return self.tmz.post('ingest', data)

    def convert_row(self, r: Dict[str, Any]) -> Dict[str, Any]:
        d = {}
        for k in r.keys():
            if isinstance(r[k], IOBase):
                logger.debug('Creating a data_upload')
                data_upload = self.tmz.data_upload.create(file=r[k])
                logger.debug(f'Data upload created: {data_upload}')
                d[k] = data_upload['presignedUrl']
            else:
                d[k] = r[k]
        return d


class DataUpload(ThirtyMHzEndpoint):
    def check(self, item, **kwargs):
        return False

    base_url = 'data-upload'

    def create(self, file: IOBase, **kwargs):
        return super(DataUpload, self).create(**kwargs, files={
            'file': (file.name, file, 'application/pdf')
        })

    def get_data(self):
        return None


class Stats(ThirtyMHzEndpoint):
    base_url = 'stats/check/{check_id}'

    def check(self, item, **kwargs):
        return True

    def get_data(self, id):
        import_check = self.tmz.import_check.get(id=id)
        logger.debug(f'Got import check: {pformat(import_check)}')
        return {
            'checkId': import_check['checkId'],
        }

    def get(self, id, **kwargs):
        send_data = self.get_data(id=id)
        data = self.tmz.get(self.base_url.format(check_id=send_data['checkId']), organization=False, data={})
        logger.debug(f'Got stats data: {pformat(data)}')
        return data


class ThirtyMHz:
    api_url = 'https://api.30mhz.com/api/{base_url}/organization/{organization}'
    api_url_no_organization = 'https://api.30mhz.com/api/{base_url}'

    def __init__(self, api_key, organization):
        self.api_key = api_key
        self.organization = organization
        self.sensor_type_obj = None
        self.import_check_obj = None
        self.data_upload_obj = None
        self.stats_obj = None

    @property
    def sensor_type(self) -> SensorType:
        if not self.sensor_type_obj:
            self.sensor_type_obj = SensorType(self)
        return self.sensor_type_obj

    @property
    def import_check(self) -> ImportCheck:
        if not self.import_check_obj:
            self.import_check_obj = ImportCheck(self)
        return self.import_check_obj

    @property
    def data_upload(self) -> DataUpload:
        if not self.data_upload_obj:
            self.data_upload_obj = DataUpload(self)
        return self.data_upload_obj

    @property
    def stats(self) -> Stats:
        if not self.stats_obj:
            self.stats_obj = Stats(self)
        return self.stats_obj

    def create_url(self, base_url, organization=True):
        if organization:
            return self.api_url.format(base_url=base_url, organization=self.organization)
        else:
            return self.api_url_no_organization.format(base_url=base_url)

    @property
    def headers(self):
        return {
            'Authorization': self.api_key,
            'Content-type': 'application/json', 'Accept': 'application/json'
        }

    def get(self, base_url, organization=True, data=None):
        url = self.create_url(base_url, organization=organization)
        r = requests.get(url, data=data, headers=self.headers)
        if 200 <= r.status_code < 300:
            return r.json()
        else:
            raise ThirtyMHzError(f"Faulty status code {r.status_code}: {r.json()}")

    def post(self, base_url, data=None, files=None):
        url = self.create_url(base_url)
        if not files:
            data = json.dumps(data)
            return requests.post(url, data=data, headers=self.headers, files=files).json()
        else:
            headers = self.headers
            del headers['Content-type']
            r = requests.post(url, data=data, headers=headers, files=files)
            if 200 <= r.status_code < 300:
                return r.json()
            else:
                raise ThirtyMHzError(f"Faulty status code {r.status_code}: {r.json()}")


class ThirtyMHzTarget(Target):
    def __init__(self, api_key, organization, **kwargs):
        super(ThirtyMHzTarget, self).__init__(**kwargs)
        self.tmz = ThirtyMHz(api_key, organization)

    def __enter__(self):
        return super(ThirtyMHzTarget, self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super(ThirtyMHzTarget, self).__exit__(exc_type, exc_val, exc_tb)

    def write(self, rows):
        super(ThirtyMHzTarget, self).write(rows)
        import_checks = {}
        import_rows = defaultdict(list)
        for row in rows:
            # Create necessary sensor types
            if not self.tmz.sensor_type.exists(id=row['id']):
                logger.debug('Creating sensor type')
                c = self.tmz.sensor_type.create(id=row['id'], name=row['name'],
                                                data=row['data'])
                logger.debug(f'Created sensor type: {c}')
            logger.debug('Getting sensor type')
            sensor_type = self.tmz.sensor_type.get(id=row['id'])

            # Create necessary import checks
            if not self.tmz.import_check.exists(id=row['id']):
                logger.debug('Creating import check')
                c = self.tmz.import_check.create(id=row['id'], name=row['name'],
                                                 sensor_type=sensor_type)
            logger.debug('Getting import check')
            import_check = self.tmz.import_check.get(id=row['id'])
            import_checks[import_check['checkId']] = import_check
            import_rows[import_check['checkId']].append(row)
        for check_id, import_check_rows in import_rows.items():
            c = self.tmz.import_check.ingest(import_checks[check_id], import_check_rows)

import json
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
        pass

    def exists(self, **kwargs):
        l = self.list()
        for i in l:
            if self.check(i, **kwargs):
                return True
        return False

    def create(self, **kwargs):
        return self.tmz.post(self.base_url, self.get_data(**kwargs))

    @abstractmethod
    def get_data(self, item):
        pass

    def get(self, **kwargs):
        l = self.list()
        print(l)
        for i in l:
            if self.check(i, **kwargs):
                return i
        return None


class SensorType(ThirtyMHzEndpoint):
    base_url = 'sensor-type'

    def check(self, item, **kwargs):
        return str(item['radioId']) == str(kwargs['id'])

    def get_data(self, id, description, data):
        json_keys = set()
        for i in data:
            json_keys.update(i.keys())
        d = {
            'name': description,
            'description': description,
            'external': True,
            'jsonKeys': list(json_keys),
            'dataTypes': ['double'] * len(json_keys),
            'radioId': id
        }
        return d


class ImportCheck(ThirtyMHzEndpoint):
    base_url = 'import-check'

    def check(self, item, **kwargs):
        return item['sourceId'] == str(kwargs['id'])

    def get_data(self, id, description, sensor_type):
        json_keys = set()
        d = {
            'name': description,
            'description': description,
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
            for r in row['result_data']:
                data.append({
                    'checkId': import_check['checkId'],
                    'data': r,
                    'timestamp': now_str,
                    'status': 'ok'
                })
        return self.tmz.post('ingest', data)


class ThirtyMHz:
    api_url = 'https://api.30mhz.com/api/{base_url}/organization/{organization}'

    def __init__(self, api_key, organization):
        self.api_key = api_key
        self.organization = organization
        self.sensor_type_obj = None
        self.import_check_obj = None

    @property
    def sensor_type(self):
        if not self.sensor_type_obj:
            self.sensor_type_obj = SensorType(self)
        return self.sensor_type_obj

    @property
    def import_check(self):
        if not self.import_check_obj:
            self.import_check_obj = ImportCheck(self)
        return self.import_check_obj

    def create_url(self, base_url):
        return self.api_url.format(base_url=base_url, organization=self.organization)

    @property
    def headers(self):
        return {
            'Authorization': self.api_key,
            'Content-type': 'application/json', 'Accept': 'application/json'
        }

    def get(self, base_url, data=None):
        url = self.create_url(base_url)
        return requests.get(url, data=data, headers=self.headers).json()

    def post(self, base_url, data=None):
        url = self.create_url(base_url)
        print(data)
        return requests.post(url, data=json.dumps(data), headers=self.headers).json()


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
            if not self.tmz.sensor_type.exists(id=row['result_group_id']):
                self.tmz.sensor_type.create(id=row['result_group_id'], description=row['result_group_description'],
                                            data=row['result_data'])
            sensor_type = self.tmz.sensor_type.get(id=row['result_group_id'])

            # Create necessary import checks
            if not self.tmz.import_check.exists(id=row['result_group_id']):
                c = self.tmz.import_check.create(id=row['result_group_id'], description=row['result_group_description'],
                                                 sensor_type=sensor_type)
            import_check = self.tmz.import_check.get(id=row['result_group_id'])
            import_checks[import_check['checkId']] = import_check
            import_rows[import_check['checkId']].append(row)
        for check_id, import_check_rows in import_rows.items():
            c = self.tmz.import_check.ingest(import_checks[check_id], import_check_rows)
            print(c)

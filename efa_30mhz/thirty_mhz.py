import json
from io import IOBase
from pprint import pformat
from typing import IO, Dict, Any

from loguru import logger
from abc import ABC, abstractmethod
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
    def get_data(self, **kwargs):
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


def infer_type(col):
    if isinstance(col, float) or isinstance(col, int):
        return 'double'
    if isinstance(col, IOBase) or isinstance(col, str):
        return 'string'


class SensorType(ThirtyMHzEndpoint):
    base_url = 'sensor-type'

    def check(self, item, **kwargs):
        return str(item['radioId']) == str(kwargs['id'])

    def get_data(self, id: str, name: str, schema: Dict):
        json_keys = []
        json_labels = []
        data_types = []
        metrics = []
        for key, val in schema.items():
            json_keys.append(key)
            json_labels.append(val['name'])
            data_types.append(val['type'])
            metrics.append(val.get('metric', 'ph'))

        logger.debug(
            f'Data types: {data_types}. Json keys: {json_keys}. Json labels: {json_labels}. Metrics: {metrics}.')
        d = {
            'name': name,
            'description': name,
            'external': True,
            'jsonKeys': json_keys,
            'jsonLabels': json_labels,
            'dataTypes': data_types,  # ['double'] * len(json_keys),
            'metrics': metrics,
            'radioId': id
        }
        return d

    def get_data_types(self, data):
        types = {}
        for d in data:
            for col in d.keys():
                proposed_type = infer_type(d[col])
                # Type already exists
                if types.get(col, None) and types.get(col) != proposed_type:
                    raise ThirtyMHzError(f'Types for {col} don\'t correspond')
                else:
                    types[col] = proposed_type
        return list(types.keys()), list(types.values())


class ImportCheck(ThirtyMHzEndpoint):
    base_url = 'import-check'

    def check(self, item, **kwargs):
        return item['sourceId'] == str(kwargs['id'])

    def get_data(self, id, name, sensor_type):
        d = {
            'name': name,
            'description': name,
            'sensorType': sensor_type['typeId'],
            'enabled': True,
            'sourceId': id,
            'timezone': 'Europe/Amsterdam',
            'notificationRelevance': 300,
            'locationId': '1'
        }
        return d

    def ingest(self, import_check, rows):
        data = []
        for r in rows:
            timestamp = r.pop('datetime').replace(microsecond=0).isoformat()
            converted_r = self.convert_row(r)
            data.append({
                'checkId': import_check['checkId'],
                'data': converted_r,
                'timestamp': timestamp,
                'status': 'ok'
            })

        logger.debug(f'Ingesting data: {pformat(data)}')
        r = self.tmz.post('ingest', data)
        if r['failedEventsNo'] > 0:
            raise ThirtyMHzError(f'Failed ingest events: {r}')

    def convert_row(self, r: Dict[str, Any]) -> Dict[str, Any]:
        d = {}
        for k in r.keys():
            if isinstance(r[k], IOBase):
                logger.debug('Creating a data_upload')
                data_upload = self.tmz.data_upload.create(file=r[k])
                logger.debug(f'Data upload created: {data_upload}')
                d[k] = data_upload['dataUploadId']
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
        headers = self.headers
        if not files:
            data = json.dumps(data)
        else:
            del headers['Content-type']
        r = requests.post(url, data=data, headers=headers, files=files)
        if 200 <= r.status_code < 300:
            logger.debug(r.json())
            return r.json()
        else:
            logger.debug('Something wrong')
            logger.debug(headers)
            logger.debug(files)
            raise ThirtyMHzError(f"Faulty status code {r.status_code}: {r.json()}")


class ThirtyMHzTarget(Target):
    def __init__(self, api_key, organization, already_done_out, **kwargs):
        super(ThirtyMHzTarget, self).__init__(**kwargs)
        self.tmz = ThirtyMHz(api_key, organization)
        self.already_done_out = already_done_out

    def write(self, rows):
        sensor_types, import_checks, ingests, ids = rows
        self.write_sensor_types(sensor_types)
        self.write_import_checks(import_checks)
        self.write_ingests(ingests)
        self.write_ids(ids)

    def write_sensor_types(self, sensor_types):
        for sensor_type in sensor_types:
            id = sensor_type['id']
            if not self.tmz.sensor_type.exists(id=sensor_type['id']):
                logger.debug('Creating sensor type')
                self.tmz.sensor_type.create(id=id, name=sensor_type['name'], schema=sensor_type['schema'])

    def write_import_checks(self, import_checks):
        for import_check in import_checks:
            if not self.tmz.import_check.exists(id=import_check['id']):
                logger.debug('Creating import check')
                sensor_type = self.tmz.sensor_type.get(id=import_check['sensor_type'])
                self.tmz.import_check.create(id=import_check['id'], name=import_check['name'], sensor_type=sensor_type)

    def write_ingests(self, ingests):
        for ingest in ingests:
            import_check = self.tmz.import_check.get(id=ingest['id'])
            self.tmz.import_check.ingest(import_check, ingest['data'])

    def write_ids(self, ids):
        with open(self.already_done_out, 'w') as f:
            logger.debug('Writing to already done')
            logger.debug(ids)
            f.write('\n'.join(list(map(str, ids))))

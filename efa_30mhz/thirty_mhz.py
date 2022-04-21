import json
import time
from io import IOBase
from pprint import pformat
from typing import IO, Dict, Any

from loguru import logger
from abc import ABC, abstractmethod
from pandas import DataFrame, concat
import requests
from datetime import datetime, timedelta, date

from efa_30mhz.metrics import Metric
from efa_30mhz.sync import Target
import efa_30mhz.constants as cst


class ThirtyMHzEndpoint(ABC):
    base_url = ""
    stats_success = None
    stats_failures = None
    stats_time = None

    def __init__(self, tmz: "ThirtyMHz"):
        self.tmz = tmz
        self.statsd_client = Metric.client()

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

    def create(self, files=None, organization=True, **kwargs):
        try:
            t0 = time.time()
            result = self.tmz.post(
                self.base_url,
                self.get_data(**kwargs),
                files=files,
                organization=organization,
            )
            t1 = time.time()
            self.statsd_client.incr(self.stats_success)
            self.statsd_client.timing(self.stats_time, t1 - t0)
            return result
        except ThirtyMHzError as e:
            logger.error(e.message)
            self.statsd_client.incr(self.stats_failures)

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
        return "double"
    if isinstance(col, IOBase) or isinstance(col, str):
        return "string"


class SensorType(ThirtyMHzEndpoint):
    base_url = "sensor-type"
    stats_success = cst.STATS_30MHZ_SENSOR_TYPES_SUCCESS
    stats_failures = cst.STATS_30MHZ_SENSOR_TYPES_FAILURES
    stats_time = cst.STATS_30MHZ_SENSOR_TYPES_TIME

    def check(self, item, **kwargs):
        return str(item["radioId"]) == str(kwargs["id"])

    def get_data(self, id: str, name: str, schema: Dict):
        json_keys = []
        json_labels = []
        data_types = []
        metrics = []
        for key, val in schema.items():
            json_keys.append(key)
            json_labels.append(val["name"])
            data_types.append(val["type"])
            metrics.append(val.get("metric", "ph"))

        logger.debug(
            f"Data types: {data_types}. Json keys: {json_keys}. Json labels: {json_labels}. Metrics: {metrics}."
        )
        d = {
            "name": name,
            "description": name,
            "external": True,
            "jsonKeys": json_keys,
            "jsonLabels": json_labels,
            "dataTypes": data_types,  # ['double'] * len(json_keys),
            "metrics": metrics,
            "radioId": id,
        }
        return d

    def get_data_types(self, data):
        types = {}
        for d in data:
            for col in d.keys():
                proposed_type = infer_type(d[col])
                # Type already exists
                if types.get(col, None) and types.get(col) != proposed_type:
                    raise ThirtyMHzError(f"Types for {col} don't correspond")
                else:
                    types[col] = proposed_type
        return list(types.keys()), list(types.values())


class ImportCheck(ThirtyMHzEndpoint):
    base_url = "import-check"
    stats_success = cst.STATS_30MHZ_IMPORT_CHECKS_SUCCESS
    stats_failures = cst.STATS_30MHZ_IMPORT_CHECKS_FAILURES
    stats_time = cst.STATS_30MHZ_IMPORT_CHECKS_TIME

    def check(self, item, **kwargs):
        return item["sourceId"] == str(kwargs["id"])

    def get_data(self, id, name, sensor_type):
        d = {
            "name": name,
            "description": name,
            "sensorType": sensor_type["typeId"],
            "enabled": None,
            "sourceId": id,
            "timezone": "Europe/Amsterdam",
            "notificationRelevance": 300,
            "locationId": "1",
        }
        return d

    def ingest(self, import_check, rows):
        t0 = time.time()
        data = []
        for r in rows:
            timestamp = r.pop("datetime").replace(microsecond=0).isoformat()
            converted_r = self.convert_row(r)
            data.append(
                {
                    "checkId": import_check["checkId"],
                    "data": converted_r,
                    "timestamp": timestamp,
                    "status": "ok",
                }
            )

        r = self.tmz.post("ingest", data)
        t1 = time.time()
        if r["failedEventsNo"] > 0:
            self.statsd_client.incr(
                cst.STATS_30MHZ_INGESTS_FAILURES, r["failedEventsNo"]
            )
            raise ThirtyMHzError(f"Failed ingest events: {r}")
        self.statsd_client.incr(cst.STATS_30MHZ_INGESTS_SUCCESS, r["okEventsNo"])
        self.statsd_client.timing(cst.STATS_30MHZ_INGESTS_TIME, t1 - t0)

    def convert_row(self, r: Dict[str, Any]) -> Dict[str, Any]:
        d = {}
        for k in r.keys():
            if isinstance(r[k], IOBase):
                logger.debug("Creating a data_upload")
                data_upload = self.tmz.data_upload.create(file=r[k])
                logger.debug(f"Data upload created: {data_upload}")
                d[k] = data_upload["dataUploadId"]
            else:
                d[k] = r[k]
        return d

class DataUpload(ThirtyMHzEndpoint):
    stats_success = cst.STATS_30MHZ_UPLOADS_SUCCESS
    stats_failures = cst.STATS_30MHZ_UPLOADS_FAILURES
    stats_time = cst.STATS_30MHZ_UPLOADS_TIME

    base_url = "data-upload"

    def check(self, item, **kwargs):
        return False

    def create(self, file: IOBase, **kwargs):
        s = super(DataUpload, self).create(
            **kwargs, files={"file": ("report.pdf", file, "application/pdf")}
        )

        file.close()
        return s

    def get_data(self):
        return None


class Stats(ThirtyMHzEndpoint):
    base_url = "stats/check/{check_id}"
    stats_success = cst.STATS_30MHZ_STATS_SUCCESS
    stats_failures = cst.STATS_30MHZ_STATS_FAILURES
    stats_time = cst.STATS_30MHZ_STATS_TIME

    def check(self, item, **kwargs):
        return True

    def get_data(self, id):
        import_check = self.tmz.import_check.get(id=id)
        # logger.debug(f"Got import check: {pformat(import_check)}")
        return {
            "checkId": import_check["checkId"],
        }

    def get(self, id, **kwargs):
        send_data = self.get_data(id=id)
        data = self.tmz.get(
            self.base_url.format(check_id=send_data["checkId"]),
            organization=False,
            data={},
        )
        # logger.debug(f"Got stats data: {pformat(data)}")
        return data


class ShareSensorType(ThirtyMHzEndpoint):
    stats_success = cst.STATS_30MHZ_SHARE_SENSOR_TYPES_SUCCESS
    stats_failures = cst.STATS_30MHZ_SHARE_SENSOR_TYPES_FAILURES
    stats_time = cst.STATS_30MHZ_SHARE_SENSOR_TYPES_TIME
    base_url = (
        "share-sensor-type/sensor-type/{sensor_type_id}/organization/{organization_id}"
    )

    def check(self, item, **kwargs):
        return True

    def get_data(self):
        return {}

    def create(self, id, organization_id):
        sensor_type = self.tmz.sensor_type.get(id=id)
        # logger.debug(f"Got sensor type: {pformat(sensor_type)}")
        prev_base_url = self.base_url
        self.base_url = self.base_url.format(
            sensor_type_id=sensor_type["typeId"], organization_id=organization_id
        )
        logger.debug(self.base_url)
        super(ShareSensorType, self).create(organization=False)
        self.base_url = prev_base_url

class ThirtyMHz:
    api_url = "https://api.30mhz.com/api/{base_url}/organization/{organization}"
    api_url_no_organization = "https://api.30mhz.com/api/{base_url}"

    def __init__(self, api_key, organization):
        self.api_key = api_key
        self.organization = organization
        self.sensor_type_obj = None
        self.share_sensor_type_obj = None
        self.import_check_obj = None
        self.data_upload_obj = None
        self.stats_obj = None

    @property
    def sensor_type(self) -> SensorType:
        if not self.sensor_type_obj:
            self.sensor_type_obj = SensorType(self)
        return self.sensor_type_obj

    @property
    def share_sensor_type(self) -> SensorType:
        if not self.share_sensor_type_obj:
            self.share_sensor_type_obj = ShareSensorType(self)
        return self.share_sensor_type_obj

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
            return self.api_url.format(
                base_url=base_url, organization=self.organization
            )
        else:
            return self.api_url_no_organization.format(base_url=base_url)

    @property
    def headers(self):
        return {
            "Authorization": self.api_key,
            "Content-type": "application/json",
            "Accept": "application/json",
        }

    def get(self, base_url, organization=True):
        url = self.create_url(base_url, organization=organization)
        headers = self.headers
        r = requests.get(url, headers=headers)
        if 200 <= r.status_code < 300:
            return r.json()
        else:
            logger.error(r.request.headers)
            raise ThirtyMHzError(f"Faulty status code {r.status_code}: {r.json()}")

    def post(self, base_url, data=None, files=None, organization=True):
        url = self.create_url(base_url, organization=organization)
        headers = self.headers
        if not files:
            data = json.dumps(data)
        else:
            del headers["Content-type"]
        r = requests.post(url, data=data, headers=headers, files=files)
        if 200 <= r.status_code < 300:
            logger.debug(r.json())
            return r.json()
        else:
            logger.debug("Something wrong")
            logger.debug(headers)
            logger.debug(files)
            logger.debug(url)
            logger.debug(data)
            raise ThirtyMHzError(f"Faulty status code {r.status_code}: {r.json()}")
        
class ThirtyMHzGetter:
    def __init__(self, default_api_key, default_organization):
        self.tmzs = {}
        self.default_api_key = default_api_key
        self.default_organization = default_organization

    def get(self, row):
        api_key = row.get("api_key", self.default_api_key)
        organization = row.get("organization_id", self.default_organization)
        return self.get_by_api_key(api_key, organization)

    def get_default(self):
        return self.get_by_api_key(self.default_api_key, self.default_organization)

    def get_by_api_key(self, api_key, organization):
        if (api_key, organization) in self.tmzs:
            return self.tmzs[(api_key, organization)]
        self.tmzs[(api_key, organization)] = ThirtyMHz(api_key, organization)
        return self.tmzs[(api_key, organization)]

class ThirtyMHzTarget(Target):
    def __init__(self, api_key, organization, already_done_out, **kwargs):
        super(ThirtyMHzTarget, self).__init__(**kwargs)
        logger.debug(f"Default organization: {organization}")
        self.tmz = ThirtyMHzGetter(api_key, organization)
        self.already_done_out = already_done_out
        self.statsd_client = Metric.client()
        self.api_key = api_key
        self.organization = organization

    def check_if_org_exists(self) -> bool:
        url = f"https://api.30mhz.com/api/organization/{self.organization}"
        
        r = requests.get(url, headers= {
                                "Authorization": self.api_key,
                                "Content-type": "application/json",
                                "Accept": "application/json",
                            })
        if r.status_code > 200:
            return False
        else:
            return True

    def write(self, rows):
        sensor_types, import_checks, ingests, ids = rows
        
        self.statsd_client.incr(cst.STATS_30MHZ_SENSOR_TYPES_TODO, len(sensor_types))
        self.statsd_client.incr(cst.STATS_30MHZ_IMPORT_CHECKS_TODO, len(import_checks))
        self.statsd_client.incr(cst.STATS_30MHZ_INGESTS_TODO, len(ingests))
        logger.debug(sensor_types)
        logger.debug("import_checks:")
        logger.debug(import_checks)
        self.write_sensor_types(sensor_types)
        self.write_import_checks(import_checks)
        
        if self.check_if_org_exists()==True:
            today = date.today()
            week_ago = today - datetime.timedelta(days=7)
            try:
                filtered = ingests = filter(
                    lambda i: i['data']['datetime'] > week_ago,
                ingests
                )
                ingests = filtered 
            except Exception as e:
                logger.error(e.message)
                    
        ingest_results = self.write_ingests(ingests)
        self.write_ids(ingest_results)


    def write_sensor_types(self, sensor_types):
        for sensor_type in sensor_types:
            id = sensor_type["id"]
            try:
                if not self.tmz.get(sensor_type).sensor_type.exists(
                    id=sensor_type["id"]
                ):
                    if not self.tmz.get_default().sensor_type.exists(
                        id=sensor_type["id"]
                    ):
                        logger.debug("Creating sensor type")
                        try:
                            self.tmz.get_default().sensor_type.create(
                                id=id,
                                name=sensor_type["name"],
                                schema=sensor_type["schema"],
                            )
                        except ThirtyMHzError as e:
                            logger.error(e)
                    logger.debug("Sharing sensor type")
                    try:
                        self.tmz.get_default().share_sensor_type.create(
                            id=id,
                            organization_id=sensor_type.get(
                                "organization_id", self.tmz.default_organization
                            ),
                        )
                    except ThirtyMHzError as e:
                        logger.error(e)
            except ThirtyMHzError as e:
                logger.error(e.message)
                logger.error(self.tmz.get_default().api_key)
                continue

    def write_import_checks(self, import_checks):
        for import_check in import_checks:
            try:
                if not self.tmz.get(import_check).import_check.exists(
                    id=import_check["id"]
                ):
                    logger.debug("Creating import check")
                    sensor_type = self.tmz.get_default().sensor_type.get(
                        id=import_check["sensor_type"]
                    )
                    if sensor_type is None:
                        logger.error(
                            f'No sensor type found: {import_check["sensor_type"]}'
                        )

                        continue
                    try:
                        self.tmz.get(import_check).import_check.create(
                            id=import_check["id"],
                            name=import_check["name"],
                            sensor_type=sensor_type,
                        )
                    except ThirtyMHzError as e:
                        logger.error(e)

            except ThirtyMHzError as e:
                logger.error(self.tmz.get(import_check).api_key)
                logger.error(e)

    def write_ingests(self, ingests):
        done_ids = []
        
        ingests = self.filter_existing_order_sample_data_ids(ingests)
        
        for ingest in ingests:
            try:
                import_check = self.tmz.get(ingest).import_check.get(id=ingest["id"])
            except ThirtyMHzError as e:
                logger.debug(e.message)
                logger.debug(self.tmz.get(ingest).api_key)
                continue
            if import_check is None:
                logger.error(f'No import check found: {ingest["id"]}')
                logger.error(ingest)
                continue
            try:
                self.tmz.get(ingest).import_check.ingest(import_check, ingest["data"])
                done_ids.append(ingest["order_id"])
            except ThirtyMHzError as e:
                logger.error(e.message)
        return done_ids
    
    def filter_existing_order_sample_data_ids(self, ingests):
        try:
            samples_getter = SamplesGetter(self.api_key, self.organization)
            
            from_date = datetime.today().strftime('%Y-%m-%d')
            to_date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
            existing_order_ids = samples_getter.get_all_order_ids_for_user_from_until(from_date, to_date)
            
            filtered_ingests = filter(
                lambda i: i['data']['order_sample_data_id'] not in existing_order_ids, 
                ingests
                )    
            
            return filtered_ingests
        
        except Exception as e:
            logger.error(e.message)
            return ingests
            

    def write_ids(self, ids):
        with open(self.already_done_out, "w") as f:
            logger.debug("Writing to already done")
            logger.debug(ids)
            f.write("\n".join(list(map(str, ids))))

class SamplesGetter:
    """
    Class for getting raw samples from 30Mhz API.
    """
    
    def __init__(self, api_key, organization):
        self.api_key = api_key
        self.organization = organization
        self.import_check_url = f"https://api.30mhz.com/api/import-check/organization/{self.organization}"
        self.stats_url = "https://api.30mhz.com/api/stats/check/{import_check_id}/from/{from_date}/until/{end_date}"
        
    def get_all_samples_for_user_from_until(self, from_date, end_date) -> DataFrame:
        
        column_names = ['timestamp', 'sensor_type', 'import_check', 'check_name', 'research_number', 'sample_description', 'file', 'order_sample_data_id']
        
        samples = DataFrame(columns=column_names)
        
        import_checks = self._get_import_checks()
        
        for import_check in import_checks:
            import_check_id = import_check['checkId']
            headers = self.headers
            params = self.stats_params
            stats_url = self.stats_url.format(import_check_id=import_check_id, from_date=from_date, end_date=end_date)
            
            r = requests.get(stats_url, headers=headers, params=params)
            
            data = r.json()
            
            samples_of_import_check = [
                self.extract_sample_identifier_data(
                    sensor_update,
                    import_check['sensorType'],
                    import_check_id,
                    import_check['name'],
                    datetime.utcfromtimestamp(int(key)/1000).strftime('%Y-%m-%d %H:%M:%S')
                ) 
                for key,sensor_update in data.items()
                    if import_check['sensorType'] + '.research_number' in sensor_update
                    and import_check['sensorType'] + '.order_sample_data_id' in sensor_update
            ]
            
            fey = DataFrame(samples_of_import_check, columns=column_names)
            
            samples = concat([samples, fey])
            
        return samples
    
    def get_all_order_ids_for_user_from_until(self, from_date, end_date) -> list:
        df = self.get_all_samples_for_user_from_until(from_date, end_date)
        return df['order_sample_data_id'].tolist()

            
    @staticmethod
    def extract_sample_identifier_data(sensor_update: dict, sensor_type: str, import_check: str, check_name: str, key: str):
        # Checks if sensor import check belongs to EFA pipeline
        return [
            key,
            sensor_type,
            import_check,
            check_name,
            sensor_update[sensor_type + '.research_number'],
            sensor_update[sensor_type + '.sample_description'],
            sensor_update[sensor_type + '.file'],
            sensor_update[sensor_type + '.order_sample_data_id']
        ]
    
    @property
    def stats_params(self):
        params = {
            "intervalSize": "15m",
            "fields": "1d",
            "statisticType": "none"
        }
        return '&'.join([k if v is None else f"{k}={v}" for k, v in params.items()]) 
        


    @property
    def headers(self):
        return {
            "Authorization": "Bearer " + self.api_key,
            "Content-type": "application/json",
            "Accept": "application/json",
        }

    def _get_import_checks(self):
        headers = self.headers
        r = requests.get(self.import_check_url, headers=headers)
        if 200 <= r.status_code < 300:
            return r.json()
        else:
            logger.error(r.request.headers)
            raise ThirtyMHzError(f"Faulty status code {r.status_code}: {r.json()}")
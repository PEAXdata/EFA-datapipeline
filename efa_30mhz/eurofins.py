import json
from datetime import datetime, date 
from typing import List, Dict, Iterable

import pytz
from loguru import logger

from efa_30mhz.errors import EurofinsError
from efa_30mhz.metrics import Metric
from efa_30mhz.pdf import PDF
from efa_30mhz.sync import Source
from efa_30mhz.thirty_mhz import infer_type
from typing import Tuple
import efa_30mhz.constants as cst


class EurofinsSource(Source):
    def __init__(
            self,
            super_source: Source,
            auth_source: Source,
            already_done_in: str,
            package_codes: Dict[str, str],
            metrics: Dict[str, str],
            wsdl: str,
            schema_version: str,
            default_api_key: str,
            default_organization: str,
            **kwargs,
    ):
        super(EurofinsSource, self).__init__(**kwargs)
        self.already_done_in = already_done_in
        self.super_source = super_source
        self.auth_source = auth_source
        self.package_codes = package_codes
        self.metrics = metrics
        self.schema_version = schema_version
        self.pdf = PDF(wsdl)
        self.statsd_client = Metric.client()
        self.default_api_key = default_api_key
        self.default_organization = default_organization

    def to_thirty_mhz(self, rows: List) -> Tuple[List, List, List, List]:
        sensor_types = []
        import_checks = []
        for organization_id in set(map(lambda r: r["organization_id"], rows)): # gets all unique sensor types
            organization_rows = list(filter(lambda x: x['organization_id'] == organization_id, rows))
            sensor_types.extend(self.uniques_schema(
                map(self.get_sensor_type, organization_rows),
                id_column="id"
            ))
            import_checks.extend(self.uniques(map(self.get_import_check, organization_rows), id_column="id")) # gets all unique import checks
        ingests = list(filter(lambda x: x is not None, map(self.get_ingests, rows))) # gets ingests
        ids = list(map(lambda x: x["order_sample_data_id"], rows)) # ids of samples
        return sensor_types, import_checks, ingests, ids

    def is_in_scope(self, row: Dict):
        today = date.today(tzinfo=row["sample_date"].tzinfo)
        week_ago = today - today - datetime.timedelta(days=7)
        return self.package_code_to_name(row["analysis_package_code"]) is not None and row["sample_date"] > week_ago

    def package_code_to_name(self, _id):
        if _id in self.package_codes:
            return self.package_codes[_id]
        return None

    def clean_single_result_data_point(self, result_data_point):
        column_mapping = {
            "resultDescription": "result_description",
            "resultValue": "result_value",
            "originCode": "origin_code",
            "resultUnitOfMeasureDescription": "result_unit_of_measure_description",
        }
        row = {
            column_mapping[k]: v
            for k, v in result_data_point.items()
            if k in column_mapping
        }
        return row

    def clean_result_group_data(self, result_group_data):
        final_result_group_data = []
        for result_group in result_group_data:
            for result_data_point in result_group["resultData"]:
                final_result_group_data.append(
                    self.clean_single_result_data_point(result_data_point)
                )
        return final_result_group_data

    def infer_metric(self, unit_description, code):
        if code in self.metrics.keys():
            return self.metrics[code]
        return self.metrics["default"]

    def get_sensor_type(self, row: Dict):
        # A sensor type is created per package code
        id = row["analysis_package_code"]
        name = self.package_code_to_name(id)
        schema = {
            "file": {
                "name": "File",
                "type": "string",
            },
            "research_number": {
                "name": "Onderzoeksnummer",
                "type": infer_type(row["sample_code"])
            }
        }
        for result_data in row["result_group_data"]:
            schema[result_data["origin_code"]] = {
                "name": result_data["result_description"],
                "type": infer_type(result_data["result_value"]),
                "metric": self.infer_metric(
                    unit_description=result_data["result_unit_of_measure_description"],
                    code=result_data["origin_code"],
                ),
            }
        return {
            "id": self.get_sensor_type_id(id),
            "name": name,
            "schema": schema,
            "api_key": row["api_key"],
            "organization_id": row["organization_id"],
        }

    def uniques(self, data: Iterable[Dict], id_column) -> List[Dict]:
        done = []
        done_ids = set()
        for d in data:
            if d[id_column] not in done_ids:
                done_ids.add(d[id_column])
                done.append(d)
        return done

    def uniques_schema(self, data: Iterable[Dict], id_column) -> List[Dict]:
        done = {}
        for d in data:
            if d[id_column] not in done:
                done[d[id_column]] = d
            else:
                if len(done[d[id_column]]["schema"].keys()) < len(d["schema"].keys()):
                    done[d[id_column]] = d

        return list(done.values())

    def get_sensor_type_id(self, analysis_package_code):
        if self.schema_version:
            return f"{analysis_package_code}_v{self.schema_version}"
        else:
            return analysis_package_code

    def get_import_check(self, row: Dict):
        name = row["sample_description"]
        sensor_type = self.get_sensor_type_id(row["analysis_package_code"])
        import_check_id = self.get_import_check_id(row)
        return {
            "id": import_check_id,
            "name": name,
            "sensor_type": sensor_type,
            "api_key": row["api_key"],
            "organization_id": row["organization_id"],
        }

    def get_sample_file(self, row: Dict):
        return self.pdf.get_pdf(row)

    def get_import_check_id(self, row: Dict):
        sensor_type = self.get_sensor_type_id(row["analysis_package_code"])
        object_code = self.get_object_code(row)
        return f"{object_code} - {sensor_type}"

    def get_ingests(self, row: Dict):
        import_check_id = self.get_import_check_id(row)
        order_id = row["order_sample_data_id"]
        data = {}
        for result_data in row["result_group_data"]:
            data[result_data["origin_code"]] = result_data["result_value"]
        data["datetime"] = row["sample_date"]
        data["research_number"] = row["sample_code"]
        data["order_sample_data_id"] = row["order_sample_data_id"]
        
        try:
            sample_file = self.get_sample_file(row)
            if sample_file:
                data['file'] = sample_file
        except EurofinsError as e:
            logger.debug(e.message)
            return None
        return {
            "id": import_check_id,
            "order_id": order_id,
            "data": [data],
            "api_key": row["api_key"],
            "organization_id": row["organization_id"],
        }

    def parse_timestamp(self, timestamp):
        return datetime.strptime(timestamp, "%Y-%m-%d").replace(
            tzinfo=pytz.timezone("Europe/Amsterdam")
        )

    def clean_data(self, row: Dict) -> Dict:
        """
        Cleans a single row
        :param row: a dictionary row
        :return: row
        """
        column_mapping = {
            "orderSampleDataId": "order_sample_data_id",
            "relationId": "relation_id",
            "resourceId": "resource_id",
            "sampleId": "sample_id",
            "sampleCode": "sample_code",
            "sampleDate": "sample_date",
            "sampleDescription": "sample_description",
            "analysisPackageCode": "analysis_package_code",
            "creationDate": "creation_date",
            "mainCategory": "main_category",
            "subCategory": "sub_category",
            "resultGroupData": "result_group_data",
            "additionalFieldList": "additional_field_list",
            "createdAt": "created_at",
            "updatedAt": "updated_at",
        }
        row = {column_mapping[k]: v for k, v in row.items() if k in column_mapping}
        row["result_group_data"] = self.clean_result_group_data(
            json.loads(row["result_group_data"])
        )
        row['additional_field_list'] = json.loads(row['additional_field_list'])
        row["sample_date"] = self.parse_timestamp(row["sample_date"])
        return row

    def read_already_done(self, already_done_in):
        with open(already_done_in, "r") as f:
            already_done = set(map(int, filter(lambda x: x != "\n", f.readlines())))
            return already_done

    def read_single_user(self, auth_row):
        
        # check if user is already known
        # if no, 

        already_done = self.read_already_done(self.already_done_in)
        rows = self.super_source.read_all(auth_row['relationId'])
        self.statsd_client.gauge(cst.STATS_SOURCE_SAMPLES, len(rows))
        self.statsd_client.gauge(
            cst.STATS_SOURCE_CLIENTS, len(set(map(lambda x: x["relationId"], rows)))
        )
        logger.debug(f"Found {len(rows)} rows")
        logger.debug(auth_row)
        rows = list(
            filter(
                lambda x: x
                          and len(x["result_group_data"]) > 0
                          and x["order_sample_data_id"] not in already_done
                          and self.is_in_scope(x),
                map(
                    lambda x: self.add_auth(row=x, auth_rows=[auth_row]),
                    map(self.clean_data, rows),
                ),
            )
        )
        if len(rows) > 0:
            logger.debug(rows[0])
        self.statsd_client.gauge(cst.STATS_SOURCE_SAMPLES_TODO, len(rows))
        self.statsd_client.gauge(
            cst.STATS_SOURCE_CLIENTS_TODO,
            len(set(map(lambda x: x["relation_id"], rows))),
        )
        logger.debug(
            f"Left with {len(rows)} rows after removing already done and rows without data."
        )
        return rows

    def read_all(self):
        logger.info("Reading")
        auth_rows = self.auth_source.read_all()
        all_rows = map(self.read_single_user, auth_rows)
        return [row for rows in all_rows for row in rows]

    def add_auth(self, row, auth_rows):
        try:
            auth_row = next(
                filter(lambda x: x["relationId"] == row["relation_id"] and x["apiKey"] is not None, auth_rows)
            )
        except StopIteration:
            row["api_key"] = self.default_api_key
            row["organization_id"] = self.default_organization
            return row

        row["api_key"] = "Bearer " + auth_row["apiKey"]
        row["organization_id"] = auth_row["organisationId"]
        return row

    def get_object_code(self, row) -> str:
        try:
            object_code = next(
                filter(lambda x: x['fieldName'] == 'CDOB', row['additional_field_list'])
            )
            return str(object_code['fieldValue'])
        except StopIteration:
            return 'object_code'

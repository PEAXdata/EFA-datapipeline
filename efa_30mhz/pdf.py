import io

from loguru import logger
from zeep import Client
import base64

from efa_30mhz.errors import EurofinsError


class PDF:
    def __init__(self, wsdl):
        self.client = None
        if wsdl is not None:
            self.client = Client(wsdl)

    def get_pdf(self, row):
        if self.client is None:
            return open("application.pdf", "rb")

        logger.info(f'Getting pdf {row["resource_id"]} for client {row["relation_id"]}')
        resource_request = {
            "user": {
                "userName": row["relation_id"],
                "requesterRelationId": row["relation_id"],
            },
            "relationId": row["relation_id"],
            "resources": [
                {
                    "ResourceRequestArray": {
                        "resourceId": row["resource_id"],
                        "resourceTypeId": 3,
                    }
                }
            ],
        }
        b64_response = self.client.service.getResource(
            getResourcesRequest=resource_request
        )

        if "resources" not in b64_response or b64_response["resources"] is None:
            logger.error("No PDF found")
            raise EurofinsError(
                f'PDF not found for resourceId {row["resource_id"]}, relationId {row["relation_id"]}. {resource_request}'
            )
        b64_pdf = b64_response["resources"]["ResourceResponseArray"][0][
            "resourceContent"
        ]
        pdf = base64.decodebytes(b64_pdf.encode("utf-8"))
        f = io.BytesIO(pdf)
        return f

from zeep import Client


class PDF:
    def __init__(self, wsdl):
        self.client = Client(wsdl)

    def get_pdf(self, row):
        return self.client.service.getResource(
            getResourcesRequest={
                'user': {
                    'userName': row['relation_id'],
                    'requesterRelationId': row['relation_id']
                },
                'relationId': row['relation_id'],
                'resources': [
                    {'ResourceRequestArray': {
                        'resourceId': row['resource_id'],
                        'resourceTypeId': 3
                    }}
                ]
            }
        )

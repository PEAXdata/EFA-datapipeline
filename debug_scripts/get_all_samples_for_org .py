from efa_30mhz.thirty_mhz import SamplesGetter
from dateutil import parser

api_key = ""
organization_id = "DeliflorChrysanten"

samples_getter = SamplesGetter(api_key, organization_id)

from_date = parser.parse("2010-01-01").strftime('%Y-%m-%dT%H:%M:%SZ')
to_date = parser.parse("2021-10-01").strftime('%Y-%m-%dT%H:%M:%SZ')

samples = samples_getter.get_all_samples_for_user_from_until(from_date, to_date)

samples.to_csv("samples")

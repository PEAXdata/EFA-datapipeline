from efa_30mhz.thirty_mhz import SamplesGetter
from dateutil import parser

api_key = "eyJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJiMGRiYjhlNi1mODM2LTRiYjItYTEzMy1hN2Q1ZGZkMzU3MTAiLCJpYXQiOjE2MzUxNjQ4OTMsInN1YiI6ImF1dGgwfDYxNzZhMmRkMTUwMjU3MDA3MDkxYmY4YyIsImlzcyI6Imh0dHBzOi8vYXBpLjMwbWh6LmNvbSIsImF1ZCI6Imh0dHBzOi8vYXBpLjMwbWh6LmNvbSIsImVtYWlsIjoiZXVyb2ZpbnMtaW50ZWdyYXRpb24uYW50aHVyYS4xQDMwbWh6LmNvbSJ9.Q5JLHhgXZpetvmc27T3KZELvHU450-1zvT5KGmki7Co"
organization_id = "Anthura"

samples_getter = SamplesGetter(api_key, organization_id)

from_date = parser.parse("2021-01-01").strftime('%Y-%m-%dT%H:%M:%SZ')
to_date = parser.parse("2021-10-01").strftime('%Y-%m-%dT%H:%M:%SZ')

samples_getter.get_all_samples_for_user_from_until(from_date, to_date)


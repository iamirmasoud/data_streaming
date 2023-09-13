import json

import requests
from producers.models import Weather

Weather(12).run(month=12)

headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
json_data = json.dumps(
    {
        "key_schema": json.dumps(Weather.key_schema),
        "value_schema": json.dumps(Weather.value_schema),
        "records": [
            {
                "key": {"timestamp": 123},
                "value": {"temperature": 90, "status": "asdqw"},
            }
        ],
    }
)
resp = requests.post(
    f"{Weather.rest_proxy_url}/topics/org.chicago.cta.weather.v1",
    headers=headers,
    data=json_data,
)

try:
    resp.raise_for_status()
    print(resp.raise_for_status())
except:
    print(f"Failed to send weather data to kafka, temp:")

print(f"Sent weather data to kafka, temp:")

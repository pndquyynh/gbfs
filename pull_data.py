from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
from os import makedirs

makedirs('data/info', exist_ok=True)
makedirs('data/status', exist_ok=True)
makedirs('data/free_bike', exist_ok=True)

# copying a retry strategy from: https://www.peterbe.com/plog/best-practice-with-retries-with-requests
def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

stations_get = requests_retry_session().get('https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json')
status_get = requests_retry_session().get('https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json')
free_bike_get = requests_retry_session().get('https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/free_bike_status.json')

status_get.raise_for_status()  # should raise error and end execution if there was a connection error

status_json = status_get.json()  # should raise error if the json is unparsable 

timestamp = status_json['last_updated']

with open(f"data/info/{timestamp}_stations.json", 'w') as stations_file:
    json.dump(stations_get.json(), stations_file)

with open(f"data/status/{timestamp}_status.json", 'w') as status_file:
    json.dump(status_json, status_file)

with open(f"data/free_bike/{timestamp}_free_bike_status.json", 'w') as free_bike_file:
    json.dump(free_bike_get.json(), free_bike_file)  # Corrected indentation here

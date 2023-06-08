import requests
import json
from datetime import datetime

url_data = "https://api.data.gov.sg/v1/transport/taxi-availability"
headers = {
        'Accept': 'application/vnd.geo+json',
        'Content-Type': 'application/json',
    }
today = datetime.today()
date_time = '2023-06-07T18:08:50+08:00'

# response_API = requests.get(url=url_data, headers=headers, verify=False).json()

# result = json.dumps(response_API,indent=2)

# print(result)
# print(response_API)

# for field in response_API:
#     print(field)
#     print(response_API[field])
start_date = datetime.today()
print(start_date)
print(start_date.strftime("%Y-%m-%dT00:00:00"))
start_time = datetime.strptime(start_date.strftime("%Y-%m-%dT00:00:00"), "%Y-%m-%dT%H:%M:%S")
print(start_time)
response_API = requests.get(url=url_data+f'?date_time={start_time.strftime("%Y-%m-%dT%H:%M:%S")}', headers=headers, verify=False).json()
print(response_API)




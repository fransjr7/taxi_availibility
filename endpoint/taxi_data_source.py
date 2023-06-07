import requests
import json
from datetime import datetime, date , timedelta


url_data = "https://api.data.gov.sg/v1/transport/taxi-availability"
headers = {
        'Accept': 'application/vnd.geo+json',
        'Content-Type': 'application/json',
    }

class TaxiDS():
    def __init__(self, url:str, headers:json={}):
        # Init information for endpoint
        self.url = url
        self.headers = headers
        
    def get_current_data(self):
        res = requests.get(url=self.url, headers=self.headers, verify=False).json()
        return(res)
    
    def get_data_ts(self, datetime:datetime):
        res = requests.get(url=self.url+f'?{datetime.strftime("%Y-%m-%dT%H:%M:%S")}+08:00', headers=self.headers, verify=False).json()
        return res
    
    def get_bulk_data(self, start_date:date, end_date:date):
        start_time = datetime.strptime(start_date.strftime("%m/%d/%YT00:00:00"), "%m/%d/%YT%H:%M:%S")
        end_time = datetime.strptime(end_date.strftime("%m/%d/%YT23:59:59"), "%m/%d/%YT%H:%M:%S")
        all_res = []
        if start_time < end_time:
            all_res.append(self.get_data_ts(start_date))
            start_time = start_time + timedelta(minutes = 1)
        
        return all_res
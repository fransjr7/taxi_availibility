import requests
import json
from datetime import datetime, date , timedelta


class TaxiDS():
    def __init__(self, url:str, headers:json={}):
        # Init information for endpoint
        self.url = url
        self.headers = headers
        
    def get_current_data(self):
        res = requests.get(url=self.url, headers=self.headers, verify=False).json()
        return(res)
    
    def get_data_ts(self, datetime:datetime):
        print(f'getting data for time : {datetime.strftime("%Y-%m-%dT%H:%M:%S")}')
        res = requests.get(url=self.url+f'?date_time={datetime.strftime("%Y-%m-%dT%H:%M:%S")}', headers=self.headers, verify=False).json()
        return res
    
    def get_bulk_data(self, start_date:date, end_date:date, seconds_interval:int):
        try:
            start_time = datetime.strptime(start_date.strftime("%Y-%m-%dT00:00:00"), "%Y-%m-%dT%H:%M:%S")
            end_time = datetime.strptime(end_date.strftime("%Y-%m-%dT23:59:59"), "%Y-%m-%dT%H:%M:%S")
            all_res = []
            while start_time < end_time:
                temp = self.get_data_ts(start_time)
                all_res.append(temp)
                start_time = start_time + timedelta(seconds = seconds_interval)
            print("+=========================")
            print(len(all_res))
            return all_res
        except:
            raise("Error in getting data bulk")